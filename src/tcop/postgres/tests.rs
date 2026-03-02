use super::*;
use crate::catalog::{reset_global_catalog_for_tests, with_global_state_lock};
use crate::tcop::engine::reset_global_storage_for_tests;

fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T {
    with_global_state_lock(|| {
        reset_global_catalog_for_tests();
        reset_global_storage_for_tests();
        f()
    })
}

fn parse_bind_execute_sync_flow() -> Vec<FrontendMessage> {
    vec![
        FrontendMessage::Parse {
            statement_name: "s1".to_string(),
            query: "SELECT 1".to_string(),
            parameter_types: vec![],
        },
        FrontendMessage::Bind {
            portal_name: "p1".to_string(),
            statement_name: "s1".to_string(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        },
        FrontendMessage::Execute {
            portal_name: "p1".to_string(),
            max_rows: 0,
        },
        FrontendMessage::Sync,
    ]
}

#[test]
fn simple_query_flow_emits_ready_and_completion() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([FrontendMessage::Query {
            sql: "SELECT 1".to_string(),
        }])
    });

    assert_eq!(
        out[0],
        BackendMessage::ReadyForQuery {
            status: ReadyForQueryStatus::Idle
        }
    );
    assert!(matches!(out[1], BackendMessage::RowDescription { .. }));
    assert!(matches!(out[2], BackendMessage::DataRow { .. }));
    assert!(matches!(
        out[3],
        BackendMessage::CommandComplete { ref tag, .. } if tag == "SELECT"
    ));
    assert_eq!(
        out[4],
        BackendMessage::ReadyForQuery {
            status: ReadyForQueryStatus::Idle
        }
    );
}

#[test]
fn extended_query_flow_requires_sync_for_ready() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync(parse_bind_execute_sync_flow())
    });

    assert_eq!(
        out[0],
        BackendMessage::ReadyForQuery {
            status: ReadyForQueryStatus::Idle
        }
    );
    assert!(matches!(out[1], BackendMessage::ParseComplete));
    assert!(matches!(out[2], BackendMessage::BindComplete));
    assert!(matches!(out[3], BackendMessage::RowDescription { .. }));
    assert!(matches!(out[4], BackendMessage::DataRow { .. }));
    assert!(matches!(
        out[5],
        BackendMessage::CommandComplete { ref tag, .. } if tag == "SELECT"
    ));
    assert_eq!(
        out[6],
        BackendMessage::ReadyForQuery {
            status: ReadyForQueryStatus::Idle
        }
    );
}

#[test]
fn parse_infers_parameter_slots_when_type_oids_are_omitted() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT $1 + 1".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::DescribeStatement {
                statement_name: "s1".to_string(),
            },
            FrontendMessage::Sync,
        ])
    });

    assert!(out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::ParameterDescription { parameter_types }
                if parameter_types == &vec![0]
        )
    }));
}

#[test]
fn bind_accepts_parameters_when_parse_omits_type_oids() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT $1 + 1".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![],
                params: vec![Some(b"41".to_vec())],
                result_formats: vec![],
            },
            FrontendMessage::Execute {
                portal_name: "p1".to_string(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ])
    });

    assert!(out.iter().any(|msg| {
        matches!(msg, BackendMessage::DataRow { values } if values == &vec!["42".to_string()])
    }));
}

#[test]
fn describe_statement_uses_planned_type_metadata() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT 1::int8 AS id, 'x'::text AS name, true AS ok".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::DescribeStatement {
                statement_name: "s1".to_string(),
            },
            FrontendMessage::Sync,
        ])
    });

    let fields = out
        .iter()
        .find_map(|msg| {
            if let BackendMessage::RowDescription { fields } = msg {
                Some(fields.clone())
            } else {
                None
            }
        })
        .expect("row description should be present");
    assert_eq!(
        fields
            .iter()
            .map(|field| field.type_oid)
            .collect::<Vec<_>>(),
        vec![20, 25, 16]
    );
}

#[test]
fn bind_supports_binary_result_format_codes() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT 1::int8 AS id, true AS ok, 1.5::float8 AS score, 'x'::text AS note"
                    .to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![1],
            },
            FrontendMessage::Execute {
                portal_name: "p1".to_string(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ])
    });

    let fields = out
        .iter()
        .find_map(|msg| {
            if let BackendMessage::RowDescription { fields } = msg {
                Some(fields.clone())
            } else {
                None
            }
        })
        .expect("row description should be present");
    assert!(fields.iter().all(|field| field.format_code == 1));

    let values = out
        .iter()
        .find_map(|msg| {
            if let BackendMessage::DataRowBinary { values } = msg {
                Some(values.clone())
            } else {
                None
            }
        })
        .expect("binary data row should be present");
    assert_eq!(values.len(), 4);
    assert_eq!(values[0].as_deref(), Some(&1i64.to_be_bytes().to_vec()[..]));
    assert_eq!(values[1].as_deref(), Some(&[1u8][..]));
    assert_eq!(
        values[2].as_deref(),
        Some(&1.5f64.to_bits().to_be_bytes().to_vec()[..])
    );
    assert_eq!(values[3].as_deref(), Some(&b"x"[..]));
}

#[test]
fn bind_supports_binary_parameter_formats() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT $1::int8 + 1 AS n, $2::boolean AS ok, $3::text AS note".to_string(),
                parameter_types: vec![20, 16, 25],
            },
            FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![1],
                params: vec![
                    Some(41i64.to_be_bytes().to_vec()),
                    Some(vec![1u8]),
                    Some(b"hello".to_vec()),
                ],
                result_formats: vec![],
            },
            FrontendMessage::Execute {
                portal_name: "p1".to_string(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ])
    });

    assert!(
        out.iter()
            .any(|msg| matches!(msg, BackendMessage::BindComplete))
    );
    assert!(out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::DataRow { values }
                if values == &vec!["42".to_string(), "t".to_string(), "hello".to_string()]
        )
    }));
}

#[test]
fn bind_supports_binary_date_and_timestamp_parameters() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT $1::date AS d, $2::timestamp AS ts".to_string(),
                parameter_types: vec![1082, 1114],
            },
            FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![1],
                params: vec![
                    Some(
                        parse_pg_date_days("2024-01-02")
                            .expect("date should parse")
                            .to_be_bytes()
                            .to_vec(),
                    ),
                    Some(
                        parse_pg_timestamp_micros("2024-01-02 03:04:05")
                            .expect("timestamp should parse")
                            .to_be_bytes()
                            .to_vec(),
                    ),
                ],
                result_formats: vec![],
            },
            FrontendMessage::Execute {
                portal_name: "p1".to_string(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ])
    });

    assert!(out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::DataRow { values }
                if values
                    == &vec!["2024-01-02".to_string(), "2024-01-02 03:04:05".to_string()]
        )
    }));
}

#[test]
fn extended_protocol_error_skips_until_sync() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: "bad".to_string(),
                query: "SELECT FROM".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT 1".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Sync,
            FrontendMessage::Parse {
                statement_name: "s2".to_string(),
                query: "SELECT 2".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Sync,
        ])
    });

    assert!(matches!(out[1], BackendMessage::ErrorResponse { .. }));
    assert!(matches!(out[2], BackendMessage::ReadyForQuery { .. }));
    assert!(matches!(out[3], BackendMessage::ParseComplete));
    assert!(matches!(out[4], BackendMessage::ReadyForQuery { .. }));
}

#[test]
fn parse_errors_include_sqlstate_and_position_metadata() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([FrontendMessage::Query {
            sql: "SELECT FROM".to_string(),
        }])
    });

    let (code, position, message) = out
        .iter()
        .find_map(|msg| {
            if let BackendMessage::ErrorResponse {
                code,
                position,
                message,
                ..
            } = msg
            {
                Some((code.clone(), *position, message.clone()))
            } else {
                None
            }
        })
        .expect("parse error should be emitted");
    assert_eq!(code, "42601");
    assert!(position.is_some());
    assert!(message.contains("parse error"));
}

#[test]
fn copy_text_from_stdin_and_to_stdout_round_trip() {
    with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([FrontendMessage::Query {
            sql: "CREATE TABLE copy_text_t (id int8, note text, ok boolean, score float8)"
                .to_string(),
        }]);

        let start = session.run_sync([FrontendMessage::Query {
            sql: "COPY copy_text_t FROM STDIN".to_string(),
        }]);
        assert!(start.iter().any(|message| {
            matches!(
                message,
                BackendMessage::CopyInResponse {
                    overall_format: 0,
                    column_formats
                } if column_formats.iter().all(|format| *format == 0)
            )
        }));

        let finish = session.run_sync([
            FrontendMessage::CopyData {
                data: b"1\talpha\ttrue\t1.5\n2\tbeta\tfalse\t2.5\n".to_vec(),
            },
            FrontendMessage::CopyDone,
        ]);
        assert!(finish.iter().any(|message| {
            matches!(
                message,
                BackendMessage::CommandComplete { tag, rows } if tag == "COPY" && *rows == 2
            )
        }));

        let copy_out = session.run_sync([FrontendMessage::Query {
            sql: "COPY copy_text_t TO STDOUT".to_string(),
        }]);
        assert!(copy_out.iter().any(|message| {
            matches!(
                message,
                BackendMessage::CopyOutResponse {
                    overall_format: 0,
                    column_formats
                } if column_formats.iter().all(|format| *format == 0)
            )
        }));
        let payload = copy_out
            .iter()
            .find_map(|message| {
                if let BackendMessage::CopyData { data } = message {
                    Some(data.clone())
                } else {
                    None
                }
            })
            .expect("copy data payload should be present");
        let rendered = String::from_utf8(payload).expect("copy text payload should be utf8");
        assert!(rendered.contains("1\talpha\tt\t1.5\n"));
        assert!(rendered.contains("2\tbeta\tf\t2.5\n"));
    });
}

#[test]
fn copy_csv_from_stdin_and_to_stdout_round_trip() {
    with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([FrontendMessage::Query {
            sql: "CREATE TABLE copy_csv_t (id int8, note text, ok boolean)".to_string(),
        }]);

        let start = session.run_sync([FrontendMessage::Query {
            sql: "COPY copy_csv_t FROM STDIN CSV".to_string(),
        }]);
        assert!(start.iter().any(|message| {
            matches!(
                message,
                BackendMessage::CopyInResponse {
                    overall_format: 0,
                    column_formats
                } if column_formats.iter().all(|format| *format == 0)
            )
        }));

        let finish = session.run_sync([
            FrontendMessage::CopyData {
                data: b"1,\"hello,world\",true\n2,\"quote\"\"inside\",false\n".to_vec(),
            },
            FrontendMessage::CopyDone,
        ]);
        assert!(finish.iter().any(|message| {
            matches!(
                message,
                BackendMessage::CommandComplete { tag, rows } if tag == "COPY" && *rows == 2
            )
        }));

        let verify = session.run_sync([FrontendMessage::Query {
            sql: "SELECT note FROM copy_csv_t WHERE id = 2".to_string(),
        }]);
        assert!(verify.iter().any(|message| {
            matches!(
                message,
                BackendMessage::DataRow { values } if values == &vec!["quote\"inside".to_string()]
            )
        }));

        let copy_out = session.run_sync([FrontendMessage::Query {
            sql: "COPY copy_csv_t TO STDOUT CSV".to_string(),
        }]);
        let payload = copy_out
            .iter()
            .find_map(|message| {
                if let BackendMessage::CopyData { data } = message {
                    Some(data.clone())
                } else {
                    None
                }
            })
            .expect("copy data payload should be present");
        let rendered = String::from_utf8(payload).expect("copy csv payload should be utf8");
        assert!(rendered.contains("\"hello,world\""));
        assert!(rendered.contains("\"quote\"\"inside\""));
    });
}

#[test]
fn aborted_transaction_block_allows_only_exit() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Parse {
                statement_name: "bad".to_string(),
                query: "SELECT FROM".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Sync,
            FrontendMessage::Query {
                sql: "SELECT 1".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK".to_string(),
            },
        ])
    });

    assert!(out.iter().any(|m| {
        matches!(
            m,
            BackendMessage::ErrorResponse { message, .. }
                if message.contains("aborted")
        )
    }));
    assert!(out.iter().any(|m| {
        matches!(
            m,
            BackendMessage::CommandComplete { tag, .. } if tag == "ROLLBACK"
        )
    }));
}

#[test]
fn rollback_restores_engine_state() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE TABLE t (id int8)".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT * FROM t".to_string(),
            },
        ])
    });

    assert!(out.iter().any(|m| {
        matches!(
            m,
            BackendMessage::CommandComplete { tag, .. } if tag == "ROLLBACK"
        )
    }));
    assert!(out.iter().any(|m| {
        matches!(
            m,
            BackendMessage::ErrorResponse { message, .. } if message.contains("does not exist")
        )
    }));
}

#[test]
fn rollback_to_savepoint_restores_partial_state() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE TABLE t (id int8)".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "SAVEPOINT s1".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (2)".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK TO SAVEPOINT s1".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT * FROM t ORDER BY 1".to_string(),
            },
            FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            },
        ])
    });

    let data_rows = out
        .iter()
        .filter_map(|message| {
            if let BackendMessage::DataRow { values } = message {
                Some(values.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(data_rows, vec![vec!["1".to_string()]]);
}

#[test]
fn release_savepoint_keeps_prior_frames() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE TABLE t (id int8)".to_string(),
            },
            FrontendMessage::Query {
                sql: "SAVEPOINT s1".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "SAVEPOINT s2".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (2)".to_string(),
            },
            FrontendMessage::Query {
                sql: "RELEASE SAVEPOINT s2".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK TO SAVEPOINT s1".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT * FROM t ORDER BY 1".to_string(),
            },
            FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            },
        ])
    });

    assert!(
        !out.iter()
            .any(|message| matches!(message, BackendMessage::DataRow { .. }))
    );
}

#[test]
fn failed_transaction_can_recover_with_rollback_to_savepoint() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "SAVEPOINT s1".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT FROM".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK TO SAVEPOINT s1".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT 1".to_string(),
            },
            FrontendMessage::Query {
                sql: "COMMIT".to_string(),
            },
        ])
    });

    assert!(out.iter().any(|m| {
            matches!(m, BackendMessage::ErrorResponse { message, .. } if message.contains("parse error"))
        }));
    assert!(out.iter().any(|m| {
        matches!(m, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
    }));
}

#[test]
fn refresh_materialized_view_concurrently_is_rejected_in_transaction_block() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "CREATE TABLE users (id int8 PRIMARY KEY)".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO users VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE UNIQUE INDEX uq_mv_users_id ON mv_users (id)".to_string(),
            },
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_users".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK".to_string(),
            },
        ])
    });

    assert!(out.iter().any(|m| {
        matches!(
            m,
            BackendMessage::ErrorResponse { message, .. }
                if message.contains("cannot be executed from a transaction block")
        )
    }));
    assert!(out.iter().any(|m| {
        matches!(
            m,
            BackendMessage::CommandComplete { tag, .. } if tag == "ROLLBACK"
        )
    }));
}

#[test]
fn explicit_transaction_keeps_writes_session_local_until_commit() {
    with_isolated_state(|| {
        let mut session_a = PostgresSession::new();
        let mut session_b = PostgresSession::new();

        session_a.run_sync([FrontendMessage::Query {
            sql: "CREATE TABLE t (id int8)".to_string(),
        }]);

        let a_uncommitted = session_a.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT count(*) FROM t".to_string(),
            },
        ]);
        assert!(a_uncommitted.iter().any(|m| {
            matches!(m, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
        }));

        let b_before_commit = session_b.run_sync([FrontendMessage::Query {
            sql: "SELECT count(*) FROM t".to_string(),
        }]);
        assert!(b_before_commit.iter().any(|m| {
            matches!(m, BackendMessage::DataRow { values } if values == &vec!["0".to_string()])
        }));

        session_a.run_sync([FrontendMessage::Query {
            sql: "COMMIT".to_string(),
        }]);

        let b_after_commit = session_b.run_sync([FrontendMessage::Query {
            sql: "SELECT count(*) FROM t".to_string(),
        }]);
        assert!(b_after_commit.iter().any(|m| {
            matches!(m, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
        }));
    });
}

#[test]
fn rollback_discards_session_local_writes() {
    with_isolated_state(|| {
        let mut session_a = PostgresSession::new();
        let mut session_b = PostgresSession::new();

        session_a.run_sync([FrontendMessage::Query {
            sql: "CREATE TABLE t (id int8)".to_string(),
        }]);

        session_a.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK".to_string(),
            },
        ]);

        let b_after_rollback = session_b.run_sync([FrontendMessage::Query {
            sql: "SELECT count(*) FROM t".to_string(),
        }]);
        assert!(b_after_rollback.iter().any(|m| {
            matches!(m, BackendMessage::DataRow { values } if values == &vec!["0".to_string()])
        }));
    });
}

#[test]
fn table_privileges_can_be_granted_and_revoked() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "CREATE TABLE docs (id int8)".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO docs VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE ROLE app LOGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "SET ROLE app".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT * FROM docs".to_string(),
            },
            FrontendMessage::Query {
                sql: "RESET ROLE".to_string(),
            },
            FrontendMessage::Query {
                sql: "GRANT SELECT ON TABLE docs TO app".to_string(),
            },
            FrontendMessage::Query {
                sql: "SET ROLE app".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT * FROM docs".to_string(),
            },
            FrontendMessage::Query {
                sql: "RESET ROLE".to_string(),
            },
            FrontendMessage::Query {
                sql: "REVOKE SELECT ON TABLE docs FROM app".to_string(),
            },
            FrontendMessage::Query {
                sql: "SET ROLE app".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT * FROM docs".to_string(),
            },
        ])
    });

    let permission_errors = out
        .iter()
        .filter(|msg| {
            matches!(
                msg,
                BackendMessage::ErrorResponse { message, .. }
                    if message.contains("missing SELECT privilege")
            )
        })
        .count();
    assert_eq!(permission_errors, 2);
    assert!(out.iter().any(
        |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
    ));
}

#[test]
fn rls_policies_filter_rows_and_enforce_with_check() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "CREATE TABLE docs (id int8, owner text)".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO docs VALUES (1, 'app'), (2, 'other')".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE ROLE app LOGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "GRANT SELECT, INSERT ON TABLE docs TO app".to_string(),
            },
            FrontendMessage::Query {
                sql: "ALTER TABLE docs ENABLE ROW LEVEL SECURITY".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE POLICY p_sel ON docs FOR SELECT TO app USING (owner = 'app')"
                    .to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE POLICY p_ins ON docs FOR INSERT TO app WITH CHECK (owner = 'app')"
                    .to_string(),
            },
            FrontendMessage::Query {
                sql: "SET ROLE app".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT id FROM docs ORDER BY 1".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO docs VALUES (3, 'other')".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO docs VALUES (4, 'app')".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT id FROM docs ORDER BY 1".to_string(),
            },
        ])
    });

    let data_rows = out
        .iter()
        .filter_map(|msg| match msg {
            BackendMessage::DataRow { values } => Some(values.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        data_rows,
        vec![
            vec!["1".to_string()],
            vec!["1".to_string()],
            vec!["4".to_string()]
        ]
    );
    assert!(out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::ErrorResponse { message, .. }
                if message.contains("row-level security policy")
        )
    }));
}

#[test]
fn startup_required_session_performs_handshake_and_query() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new_startup_required();
        session.run_sync([
            FrontendMessage::Startup {
                user: "postgres".to_string(),
                database: Some("public".to_string()),
                parameters: vec![("application_name".to_string(), "tests".to_string())],
            },
            FrontendMessage::Query {
                sql: "SELECT 1".to_string(),
            },
        ])
    });

    assert!(
        out.iter()
            .any(|msg| matches!(msg, BackendMessage::AuthenticationOk))
    );
    assert!(out.iter().any(
        |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
    ));
}

#[test]
fn startup_password_authentication_round_trip() {
    with_isolated_state(|| {
        let mut admin = PostgresSession::new();
        admin.run_sync([FrontendMessage::Query {
            sql: "CREATE ROLE alice LOGIN PASSWORD 's3cr3t'".to_string(),
        }]);

        let mut session = PostgresSession::new_startup_required();
        let startup_out = session.run_sync([FrontendMessage::Startup {
            user: "alice".to_string(),
            database: None,
            parameters: Vec::new(),
        }]);
        assert!(startup_out.iter().any(|msg| matches!(
            msg,
            BackendMessage::AuthenticationCleartextPassword
        ) || matches!(
            msg,
            BackendMessage::AuthenticationSasl { .. }
        )));

        let auth_out = session.run_sync([
            FrontendMessage::Password {
                password: "s3cr3t".to_string(),
            },
            FrontendMessage::Query {
                sql: "SELECT 1".to_string(),
            },
        ]);
        assert!(
            auth_out
                .iter()
                .any(|msg| matches!(msg, BackendMessage::AuthenticationOk))
        );
        assert!(auth_out.iter().any(
                |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["1".to_string()])
            ));
    });
}

#[test]
fn security_changes_inside_transaction_rollback() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "BEGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "CREATE ROLE temp LOGIN".to_string(),
            },
            FrontendMessage::Query {
                sql: "ROLLBACK".to_string(),
            },
            FrontendMessage::Query {
                sql: "SET ROLE temp".to_string(),
            },
        ])
    });

    assert!(out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::ErrorResponse { message, .. }
                if message.contains("does not exist")
        )
    }));
}

#[test]
fn implicit_transaction_auto_rollback_prevents_cascade_failures() {
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Query {
                sql: "CREATE TABLE t1 (id int)".to_string(),
            },
            FrontendMessage::Query {
                sql: "INSERT INTO t1 VALUES (1)".to_string(),
            },
            FrontendMessage::Query {
                // This should fail (invalid SQL)
                sql: "INSERT INTO t1 VALUES invalid".to_string(),
            },
            FrontendMessage::Query {
                // This should succeed (not in aborted transaction)
                sql: "SELECT * FROM t1".to_string(),
            },
        ])
    });

    // Should have an error for the invalid INSERT
    assert!(
        out.iter()
            .any(|msg| { matches!(msg, BackendMessage::ErrorResponse { .. }) })
    );

    // Should have a successful SELECT result after the error
    assert!(out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::DataRow { values }
                if values == &vec!["1".to_string()]
        )
    }));

    // Should not have "current transaction is aborted" error
    assert!(!out.iter().any(|msg| {
        matches!(
            msg,
            BackendMessage::ErrorResponse { message, .. }
                if message.contains("current transaction is aborted")
        )
    }));
}
