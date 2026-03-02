#[allow(clippy::wildcard_imports)]
use super::*;

impl PostgresSession {
    pub(super) fn exec_startup_message(
        &mut self,
        user: String,
        database: Option<String>,
        parameters: Vec<(String, String)>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        if self.startup_complete {
            return Err(SessionError {
                message: "startup packet has already been processed".to_string(),
            });
        }
        let user = security::normalize_identifier(&user);
        if !security::role_exists(&user) {
            return Err(SessionError {
                message: format!("role \"{user}\" does not exist"),
            });
        }
        if !security::can_role_login(&user) {
            return Err(SessionError {
                message: format!("role \"{user}\" is not permitted to login"),
            });
        }

        self.pending_startup = Some(PendingStartup {
            user: user.clone(),
            database: database.clone(),
            parameters: parameters.clone(),
        });

        if let Some(password) = security::role_password(&user) {
            self.authentication_state = AuthenticationState::AwaitingSaslInitial { password };
            out.push(BackendMessage::AuthenticationSasl {
                mechanisms: vec!["SCRAM-SHA-256".to_string()],
            });
            return Ok(());
        }

        self.authentication_state = AuthenticationState::None;
        self.complete_startup(user, database, parameters, out);
        Ok(())
    }

    pub(super) fn exec_password_message(
        &mut self,
        password: String,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        if self.authentication_state != AuthenticationState::AwaitingPassword
            && !matches!(
                self.authentication_state,
                AuthenticationState::AwaitingSaslInitial { .. }
            )
        {
            return Err(SessionError {
                message: "password message not expected".to_string(),
            });
        }
        let pending = self.pending_startup.clone().ok_or_else(|| SessionError {
            message: "startup state is missing pending user".to_string(),
        })?;
        if !security::verify_role_password(&pending.user, &password) {
            return Err(SessionError {
                message: "password authentication failed".to_string(),
            });
        }
        self.complete_startup(pending.user, pending.database, pending.parameters, out);
        Ok(())
    }

    pub(super) fn exec_sasl_initial_response(
        &mut self,
        mechanism: String,
        data: Vec<u8>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        let AuthenticationState::AwaitingSaslInitial { password } = &self.authentication_state
        else {
            return Err(SessionError {
                message: "SASL initial response not expected".to_string(),
            });
        };
        if !mechanism.eq_ignore_ascii_case("SCRAM-SHA-256") {
            return Err(SessionError {
                message: format!("unsupported SASL mechanism {mechanism}"),
            });
        }

        let client_first = std::str::from_utf8(&data).map_err(|_| SessionError {
            message: "invalid SASL payload encoding".to_string(),
        })?;
        let (_gs2, client_first_bare) =
            client_first.split_once(",,").ok_or_else(|| SessionError {
                message: "invalid SCRAM client-first message".to_string(),
            })?;
        let client_nonce = scram_attribute(client_first_bare, 'r').ok_or_else(|| SessionError {
            message: "SCRAM client-first message is missing nonce".to_string(),
        })?;
        if client_nonce.is_empty() {
            return Err(SessionError {
                message: "SCRAM client nonce cannot be empty".to_string(),
            });
        }

        let mut random_nonce = [0u8; 18];
        fill_random_bytes(&mut random_nonce);
        let combined_nonce = format!("{}{}", client_nonce, BASE64_STANDARD.encode(random_nonce));

        let mut salt = [0u8; 16];
        fill_random_bytes(&mut salt);
        let iterations = 4096u32;
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            BASE64_STANDARD.encode(salt),
            iterations
        );

        self.authentication_state = AuthenticationState::AwaitingSaslResponse {
            pending: ScramPendingState {
                password: password.clone(),
                client_first_bare: client_first_bare.to_string(),
                server_first: server_first.clone(),
                combined_nonce,
                salt: salt.to_vec(),
                iterations,
            },
        };

        out.push(BackendMessage::AuthenticationSaslContinue {
            data: server_first.into_bytes(),
        });
        Ok(())
    }

    pub(super) fn exec_sasl_response(
        &mut self,
        data: Vec<u8>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        let AuthenticationState::AwaitingSaslResponse { pending } = &self.authentication_state
        else {
            return Err(SessionError {
                message: "SASL response not expected".to_string(),
            });
        };
        let client_final = std::str::from_utf8(&data).map_err(|_| SessionError {
            message: "invalid SCRAM client-final payload".to_string(),
        })?;
        let final_nonce = scram_attribute(client_final, 'r').ok_or_else(|| SessionError {
            message: "SCRAM client-final message is missing nonce".to_string(),
        })?;
        if final_nonce != pending.combined_nonce {
            return Err(SessionError {
                message: "SCRAM nonce mismatch".to_string(),
            });
        }
        let Some(client_final_without_proof) =
            client_final.rsplit_once(",p=").map(|(head, _)| head)
        else {
            return Err(SessionError {
                message: "SCRAM client-final message is missing proof".to_string(),
            });
        };
        let proof_b64 = scram_attribute(client_final, 'p').ok_or_else(|| SessionError {
            message: "SCRAM client-final proof is missing".to_string(),
        })?;
        let proof = BASE64_STANDARD
            .decode(proof_b64)
            .map_err(|_| SessionError {
                message: "SCRAM proof is not valid base64".to_string(),
            })?;
        if proof.len() != 32 {
            return Err(SessionError {
                message: "SCRAM proof has invalid length".to_string(),
            });
        }

        let auth_message = format!(
            "{},{},{}",
            pending.client_first_bare, pending.server_first, client_final_without_proof
        );
        let salted_password = pbkdf2_hmac_array::<Sha256, 32>(
            pending.password.as_bytes(),
            &pending.salt,
            pending.iterations,
        );
        let client_key = scram_hmac(&salted_password, b"Client Key")?;
        let stored_key = Sha256::digest(client_key);
        let client_signature = scram_hmac(stored_key.as_slice(), auth_message.as_bytes())?;
        let expected_client_key = proof
            .iter()
            .zip(client_signature.iter())
            .map(|(lhs, rhs)| lhs ^ rhs)
            .collect::<Vec<_>>();
        let expected_stored_key = Sha256::digest(&expected_client_key);
        if expected_stored_key.as_slice() != stored_key.as_slice() {
            return Err(SessionError {
                message: "SCRAM proof verification failed".to_string(),
            });
        }

        let server_key = scram_hmac(&salted_password, b"Server Key")?;
        let server_signature = scram_hmac(&server_key, auth_message.as_bytes())?;
        let server_final = format!("v={}", BASE64_STANDARD.encode(server_signature));
        out.push(BackendMessage::AuthenticationSaslFinal {
            data: server_final.into_bytes(),
        });

        let pending_startup = self.pending_startup.clone().ok_or_else(|| SessionError {
            message: "startup state is missing pending user".to_string(),
        })?;
        self.complete_startup(
            pending_startup.user,
            pending_startup.database,
            pending_startup.parameters,
            out,
        );
        Ok(())
    }

    pub(super) fn complete_startup(
        &mut self,
        user: String,
        database: Option<String>,
        parameters: Vec<(String, String)>,
        out: &mut Vec<BackendMessage>,
    ) {
        self.startup_complete = true;
        self.authentication_state = AuthenticationState::None;
        self.pending_startup = None;
        self.session_user = user.clone();
        self.current_role = user;
        self.send_ready_for_query = false;

        out.push(BackendMessage::AuthenticationOk);
        out.push(BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: "16.0-openassay".to_string(),
        });
        out.push(BackendMessage::ParameterStatus {
            name: "client_encoding".to_string(),
            value: "UTF8".to_string(),
        });
        out.push(BackendMessage::ParameterStatus {
            name: "DateStyle".to_string(),
            value: "ISO, MDY".to_string(),
        });
        out.push(BackendMessage::ParameterStatus {
            name: "integer_datetimes".to_string(),
            value: "on".to_string(),
        });
        if let Some(db) = database {
            out.push(BackendMessage::ParameterStatus {
                name: "database".to_string(),
                value: db,
            });
        }
        for (name, value) in parameters {
            let normalized = name.to_ascii_lowercase();
            if normalized == "user" || normalized == "database" {
                continue;
            }
            out.push(BackendMessage::ParameterStatus { name, value });
        }
        out.push(BackendMessage::BackendKeyData {
            process_id: self.process_id,
            secret_key: self.secret_key,
        });
        out.push(BackendMessage::ReadyForQuery {
            status: self.ready_status(),
        });
    }
}

fn scram_attribute(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|part| {
        let (k, value) = part.split_once('=')?;
        (k.len() == 1 && k.starts_with(key)).then_some(value)
    })
}

fn scram_hmac(key: &[u8], data: &[u8]) -> Result<[u8; 32], SessionError> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| SessionError {
        message: "SCRAM HMAC key initialization failed".to_string(),
    })?;
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn fill_random_bytes(out: &mut [u8]) {
    crate::utils::random::fill_random_bytes(out);
}
