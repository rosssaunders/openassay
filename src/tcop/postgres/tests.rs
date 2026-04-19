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
fn simple_query_caches_select_plans() {
    with_isolated_state(|| {
        let mut session = PostgresSession::new();

        session.run_sync([FrontendMessage::Query {
            sql: "SELECT 1".to_string(),
        }]);
        assert_eq!(session.simple_query_cache.len(), 1);
        assert!(session.simple_query_cache.contains_key("SELECT 1"));

        session.run_sync([FrontendMessage::Query {
            sql: "SELECT 1".to_string(),
        }]);
        assert_eq!(session.simple_query_cache.len(), 1);
    });
}

#[test]
fn simple_query_cache_clears_after_non_select_execution() {
    with_isolated_state(|| {
        let mut session = PostgresSession::new();

        session.run_sync([FrontendMessage::Query {
            sql: "SELECT 1".to_string(),
        }]);
        assert_eq!(session.simple_query_cache.len(), 1);

        session.run_sync([FrontendMessage::Query {
            sql: "CREATE TABLE cache_invalidation (id INT)".to_string(),
        }]);
        assert!(session.simple_query_cache.is_empty());
    });
}

#[test]
fn extended_query_flow_without_describe_omits_row_description() {
    // PG wire spec: Execute MUST NOT emit RowDescription. When the client skips
    // Describe, it simply won't receive column metadata.
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
    assert!(matches!(out[3], BackendMessage::DataRow { .. }));
    assert!(matches!(
        out[4],
        BackendMessage::CommandComplete { ref tag, .. } if tag == "SELECT"
    ));
    assert_eq!(
        out[5],
        BackendMessage::ReadyForQuery {
            status: ReadyForQueryStatus::Idle
        }
    );
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::RowDescription { .. })),
        "Execute must not emit RowDescription when Describe was not issued"
    );
}

#[test]
fn extended_query_flow_with_describe_portal_emits_row_description_once() {
    // Spec-correct path used by psql, tokio-postgres, and friends:
    // Parse → Bind → Describe(portal) → Execute → Sync produces one
    // RowDescription (from Describe) followed by DataRow+CommandComplete.
    let out = with_isolated_state(|| {
        let mut session = PostgresSession::new();
        session.run_sync([
            FrontendMessage::Parse {
                statement_name: String::new(),
                query: "SELECT 1".to_string(),
                parameter_types: vec![],
            },
            FrontendMessage::Bind {
                portal_name: String::new(),
                statement_name: String::new(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
            FrontendMessage::DescribePortal {
                portal_name: String::new(),
            },
            FrontendMessage::Execute {
                portal_name: String::new(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ])
    });

    let row_desc_count = out
        .iter()
        .filter(|msg| matches!(msg, BackendMessage::RowDescription { .. }))
        .count();
    assert_eq!(
        row_desc_count, 1,
        "exactly one RowDescription expected (from Describe, not Execute)"
    );
    let row_desc_idx = out
        .iter()
        .position(|msg| matches!(msg, BackendMessage::RowDescription { .. }))
        .unwrap();
    let data_row_idx = out
        .iter()
        .position(|msg| matches!(msg, BackendMessage::DataRow { .. }))
        .unwrap();
    assert!(
        row_desc_idx < data_row_idx,
        "RowDescription must precede DataRow"
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
    // Spec-correct flow: Describe(portal) comes between Bind and Execute so
    // the RowDescription is emitted by Describe (not Execute) and carries the
    // format codes from the Bind message.
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
            FrontendMessage::DescribePortal {
                portal_name: "p1".to_string(),
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

#[test]
fn time_binary_encode_roundtrips_through_decode() {
    let encoded =
        encode_binary_scalar(&ScalarValue::Text("12:34:56.789012".to_string()), 1083, "t")
            .expect("time encodes");
    assert_eq!(encoded.len(), 8);
    let expected_micros: i64 = 12 * 3_600_000_000 + 34 * 60_000_000 + 56 * 1_000_000 + 789_012;
    assert_eq!(encoded, expected_micros.to_be_bytes().to_vec());

    let decoded = decode_binary_scalar(&encoded, 1083, "t").expect("time decodes");
    assert_eq!(decoded, ScalarValue::Text("12:34:56.789012".to_string()));
}

#[test]
fn time_binary_encode_zero_fraction_formats_without_decimal() {
    let encoded =
        encode_binary_scalar(&ScalarValue::Text("00:00:00".to_string()), 1083, "t").unwrap();
    assert_eq!(encoded, 0i64.to_be_bytes().to_vec());
    let decoded = decode_binary_scalar(&encoded, 1083, "t").unwrap();
    assert_eq!(decoded, ScalarValue::Text("00:00:00".to_string()));
}

#[test]
fn interval_binary_encode_lays_out_micros_days_months() {
    // Engine text: "3 mons 14 days 02:30:00"
    let encoded = encode_binary_scalar(
        &ScalarValue::Text("3 mons 14 days 02:30:00".to_string()),
        1186,
        "i",
    )
    .expect("interval encodes");
    assert_eq!(encoded.len(), 16);
    let micros = i64::from_be_bytes(encoded[0..8].try_into().unwrap());
    let days = i32::from_be_bytes(encoded[8..12].try_into().unwrap());
    let months = i32::from_be_bytes(encoded[12..16].try_into().unwrap());
    assert_eq!(micros, 2 * 3_600_000_000 + 30 * 60_000_000);
    assert_eq!(days, 14);
    assert_eq!(months, 3);
}

#[test]
fn interval_binary_encode_roundtrips_negative_time() {
    let encoded = encode_binary_scalar(
        &ScalarValue::Text("0 mons 0 days -01:00:00".to_string()),
        1186,
        "i",
    )
    .unwrap();
    let decoded = decode_binary_scalar(&encoded, 1186, "i").unwrap();
    assert_eq!(
        decoded,
        ScalarValue::Text("0 mons 0 days -01:00:00".to_string())
    );
}

#[test]
fn interval_binary_encode_accepts_hours_over_24() {
    // The engine renders intervals without a 24-hour cap, e.g. `36:45:00`,
    // so the interval HMS parse path must not reject hours >= 24 — unlike
    // `time` which is bounded to [00:00, 24:00).
    let encoded = encode_binary_scalar(
        &ScalarValue::Text("0 mons 0 days 36:45:00".to_string()),
        1186,
        "i",
    )
    .expect("36-hour interval encodes");
    let micros = i64::from_be_bytes(encoded[0..8].try_into().unwrap());
    assert_eq!(micros, 36 * 3_600_000_000 + 45 * 60_000_000);
}

#[test]
fn int4_array_binary_encode_layout() {
    // Int4[] = [7, 8, 9] — header + 3 length-prefixed int4 payloads.
    let values = vec![
        ScalarValue::Int(7),
        ScalarValue::Int(8),
        ScalarValue::Int(9),
    ];
    let encoded =
        encode_binary_scalar(&ScalarValue::Array(values), 1007, "a").expect("int4[] encodes");
    assert_eq!(i32::from_be_bytes(encoded[0..4].try_into().unwrap()), 1); // ndim
    assert_eq!(i32::from_be_bytes(encoded[4..8].try_into().unwrap()), 0); // dataoffset
    assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 23); // int4 oid
    assert_eq!(i32::from_be_bytes(encoded[12..16].try_into().unwrap()), 3); // dim size
    assert_eq!(i32::from_be_bytes(encoded[16..20].try_into().unwrap()), 1); // lower_bound
    // first element: len=4, value=7
    assert_eq!(i32::from_be_bytes(encoded[20..24].try_into().unwrap()), 4);
    assert_eq!(i32::from_be_bytes(encoded[24..28].try_into().unwrap()), 7);
}

#[test]
fn int4_array_binary_roundtrips_with_nulls() {
    let values = vec![ScalarValue::Int(1), ScalarValue::Null, ScalarValue::Int(3)];
    let encoded = encode_binary_scalar(&ScalarValue::Array(values.clone()), 1007, "a").unwrap();
    let decoded = decode_binary_scalar(&encoded, 1007, "a").unwrap();
    match decoded {
        ScalarValue::Array(got) => {
            assert_eq!(got.len(), 3);
            assert!(matches!(got[0], ScalarValue::Int(1)));
            assert!(matches!(got[1], ScalarValue::Null));
            assert!(matches!(got[2], ScalarValue::Int(3)));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn text_array_binary_roundtrips() {
    let values = vec![
        ScalarValue::Text("hello".to_string()),
        ScalarValue::Text("world".to_string()),
    ];
    let encoded = encode_binary_scalar(&ScalarValue::Array(values.clone()), 1009, "a").unwrap();
    let decoded = decode_binary_scalar(&encoded, 1009, "a").unwrap();
    assert_eq!(decoded, ScalarValue::Array(values));
}

#[test]
fn empty_int4_array_binary_decodes() {
    let encoded = encode_binary_scalar(&ScalarValue::Array(Vec::new()), 1007, "a").unwrap();
    // Header alone = 20 bytes (ndim + dataoffset + oid + dim_size + lower_bound).
    assert_eq!(encoded.len(), 20);
    let decoded = decode_binary_scalar(&encoded, 1007, "a").unwrap();
    assert_eq!(decoded, ScalarValue::Array(Vec::new()));
}

#[test]
fn empty_array_with_ndim_zero_is_accepted_on_decode() {
    // Some PG versions emit ndim=0 for empty arrays. Construct that shape
    // by hand and make sure the decoder accepts it.
    let mut raw = Vec::new();
    raw.extend_from_slice(&0i32.to_be_bytes()); // ndim=0
    raw.extend_from_slice(&0i32.to_be_bytes()); // dataoffset
    raw.extend_from_slice(&23u32.to_be_bytes()); // int4 element oid
    let decoded = decode_binary_scalar(&raw, 1007, "a").unwrap();
    assert_eq!(decoded, ScalarValue::Array(Vec::new()));
}

#[test]
fn multidim_array_binary_decode_is_rejected() {
    let mut raw = Vec::new();
    raw.extend_from_slice(&2i32.to_be_bytes()); // ndim=2 — not supported
    raw.extend_from_slice(&0i32.to_be_bytes());
    raw.extend_from_slice(&23u32.to_be_bytes());
    let err = decode_binary_scalar(&raw, 1007, "a");
    assert!(err.is_err());
}

#[test]
fn array_element_oid_mismatch_is_rejected() {
    // Header says int4 element; decoder was asked for int8[] → reject.
    let mut raw = Vec::new();
    raw.extend_from_slice(&1i32.to_be_bytes());
    raw.extend_from_slice(&0i32.to_be_bytes());
    raw.extend_from_slice(&23u32.to_be_bytes()); // int4 — but we'll decode as int8[]
    raw.extend_from_slice(&0i32.to_be_bytes());
    raw.extend_from_slice(&1i32.to_be_bytes());
    let err = decode_binary_scalar(&raw, 1016, "a");
    assert!(err.is_err());
}

#[test]
fn numeric_binary_encode_zero() {
    use rust_decimal::Decimal;
    let bytes = encode_binary_scalar(&ScalarValue::Numeric(Decimal::ZERO), 1700, "n").unwrap();
    assert_eq!(bytes, vec![0, 0, 0, 0, 0, 0, 0, 0]); // ndigits=0 weight=0 sign=0 dscale=0
}

#[test]
fn numeric_binary_encode_one() {
    use rust_decimal::Decimal;
    let bytes = encode_binary_scalar(&ScalarValue::Numeric(Decimal::ONE), 1700, "n").unwrap();
    // ndigits=1 weight=0 sign=0 dscale=0, then digit 1
    assert_eq!(bytes, vec![0, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
}

#[test]
fn numeric_binary_encode_ten_thousand_trims_trailing_zero_digit() {
    use rust_decimal::Decimal;
    let bytes =
        encode_binary_scalar(&ScalarValue::Numeric(Decimal::from(10_000i64)), 1700, "n").unwrap();
    // Expect ndigits=1 weight=1 sign=0 dscale=0, then digit 1 (trailing 0 digit trimmed)
    assert_eq!(bytes, vec![0, 1, 0, 1, 0, 0, 0, 0, 0, 1]);
}

#[test]
fn numeric_binary_encode_small_fraction_leading_zero_trim() {
    use rust_decimal::Decimal;
    let d = Decimal::new(1, 4); // 0.0001
    let bytes = encode_binary_scalar(&ScalarValue::Numeric(d), 1700, "n").unwrap();
    // ndigits=1 weight=-1 sign=0 dscale=4, then digit 1
    // weight=-1 on the wire is 0xFFFF.
    assert_eq!(bytes, vec![0, 1, 0xFF, 0xFF, 0, 0, 0, 4, 0, 1]);
}

#[test]
fn numeric_binary_encode_negative_with_fraction() {
    use rust_decimal::Decimal;
    let d: Decimal = "-12345.678".parse().unwrap();
    let bytes = encode_binary_scalar(&ScalarValue::Numeric(d), 1700, "n").unwrap();
    // ndigits=3 weight=1 sign=0x4000 dscale=3, digits [1, 2345, 6780]
    let mut expected = Vec::new();
    expected.extend_from_slice(&3i16.to_be_bytes());
    expected.extend_from_slice(&1i16.to_be_bytes());
    expected.extend_from_slice(&0x4000u16.to_be_bytes());
    expected.extend_from_slice(&3i16.to_be_bytes());
    expected.extend_from_slice(&1u16.to_be_bytes());
    expected.extend_from_slice(&2345u16.to_be_bytes());
    expected.extend_from_slice(&6780u16.to_be_bytes());
    assert_eq!(bytes, expected);
}

#[test]
fn numeric_binary_roundtrips_advisor_cases() {
    use rust_decimal::Decimal;
    for case in [
        Decimal::ZERO,
        Decimal::ONE,
        Decimal::from(10_000i64),
        Decimal::new(1, 4), // 0.0001
        "-12345.678".parse().unwrap(),
        "3.14159265358979".parse().unwrap(),
        "-0.5".parse().unwrap(),
    ] {
        let encoded = encode_binary_scalar(&ScalarValue::Numeric(case), 1700, "n").unwrap();
        let decoded = decode_binary_scalar(&encoded, 1700, "n").unwrap();
        match decoded {
            ScalarValue::Numeric(got) => assert_eq!(got, case, "round-trip of {case} failed"),
            other => panic!("expected Numeric, got {other:?}"),
        }
    }
}

#[test]
fn numeric_binary_decode_rejects_nan() {
    let raw = vec![0, 0, 0, 0, 0xC0, 0x00, 0, 0]; // sign=0xC000
    let err = decode_binary_scalar(&raw, 1700, "n");
    assert!(err.is_err());
}

#[test]
fn numeric_binary_decode_rejects_truncated_header() {
    let raw = vec![0, 1, 0, 0];
    let err = decode_binary_scalar(&raw, 1700, "n");
    assert!(err.is_err());
}

#[test]
fn numeric_binary_encode_from_text_and_int_forms() {
    let bytes_text = encode_binary_scalar(&ScalarValue::Text("42".to_string()), 1700, "n").unwrap();
    let bytes_int = encode_binary_scalar(&ScalarValue::Int(42), 1700, "n").unwrap();
    assert_eq!(bytes_text, bytes_int);
    // Sanity: ndigits=1 weight=0 sign=0 dscale=0, digit 42.
    assert_eq!(bytes_int, vec![0, 1, 0, 0, 0, 0, 0, 0, 0, 42]);
}

#[test]
fn interval_binary_encode_rejects_unknown_text_shape() {
    let err = encode_binary_scalar(
        &ScalarValue::Text("3 months, 14 days, 02:30:00".to_string()),
        1186,
        "i",
    );
    assert!(err.is_err());
}

// Phase 4.1: SQLSTATE classifier covers the codes library tests match on
// via `tokio_postgres::error::SqlState`. Lib-level tests (deterministic —
// the integration path shares process-global catalog state with other
// tests and races.)
#[test]
fn sqlstate_classifies_unique_violation() {
    let msg = "duplicate value violates unique index \"x_pkey\"";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "23505");
}

#[test]
fn sqlstate_classifies_foreign_key_violation() {
    let msg = "insert or update on relation \"child\" violates foreign key constraint";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "23503");
}

#[test]
fn sqlstate_classifies_check_violation() {
    let msg = "row for relation \"t\" violates CHECK constraint on column \"price\"";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "23514");
}

#[test]
fn sqlstate_classifies_not_null_violation() {
    let msg = "null value in column \"x\" of relation \"t\" violates not-null constraint";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "23502");
}

#[test]
fn sqlstate_classifies_invalid_text_representation() {
    let msg = "invalid input syntax for type int4: \"abc\"";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "22P02");
}

#[test]
fn sqlstate_classifies_out_of_range() {
    let msg = "int2 value 40000 out of range";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "22003");
}

#[test]
fn sqlstate_classifies_datatype_mismatch() {
    let msg = "cannot cast text to uuid";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "42804");
}

#[test]
fn sqlstate_classifies_invalid_catalog_name() {
    let msg = "database \"nonexistent\" does not exist";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "3D000");
}

#[test]
fn sqlstate_falls_back_to_xx000_when_unmatched() {
    let msg = "an unrecognised error message shape";
    let (code, _, _, _) = classify_sqlstate_error_fields(msg);
    assert_eq!(code, "XX000");
}
