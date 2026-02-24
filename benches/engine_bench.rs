use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

fn assert_ok(out: &[BackendMessage]) {
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "benchmark query produced error: {out:?}"
    );
}

fn bench_simple_select(c: &mut Criterion) {
    let mut session = PostgresSession::new();
    c.bench_function("simple_select", |b| {
        b.iter(|| {
            let out = session.run_sync([FrontendMessage::Query {
                sql: "SELECT 1".to_string(),
            }]);
            assert_ok(&out);
        });
    });
}

fn bench_insert_throughput(c: &mut Criterion) {
    c.bench_function("insert_throughput", |b| {
        b.iter_batched(
            || {
                let mut session = PostgresSession::new();
                let out = session.run_sync([FrontendMessage::Query {
                    sql: "CREATE TABLE bench_insert (id int8, val text)".to_string(),
                }]);
                assert_ok(&out);
                session
            },
            |mut session| {
                let out = session.run_sync([FrontendMessage::Query {
                    sql: "INSERT INTO bench_insert VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
                        .to_string(),
                }]);
                assert_ok(&out);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_join_performance(c: &mut Criterion) {
    c.bench_function("join_performance", |b| {
        b.iter_batched(
            || {
                let mut session = PostgresSession::new();
                let out = session.run_sync([FrontendMessage::Query {
                    sql: "CREATE TABLE bench_left (id int8, val text); \
                         CREATE TABLE bench_right (id int8, val text); \
                         INSERT INTO bench_left VALUES (1, 'a'), (2, 'b'), (3, 'c'); \
                         INSERT INTO bench_right VALUES (1, 'x'), (2, 'y'), (3, 'z')"
                        .to_string(),
                }]);
                assert_ok(&out);
                session
            },
            |mut session| {
                let out = session.run_sync([FrontendMessage::Query {
                    sql: "SELECT l.val, r.val FROM bench_left l JOIN bench_right r ON l.id = r.id"
                        .to_string(),
                }]);
                assert_ok(&out);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_expression_eval(c: &mut Criterion) {
    let mut session = PostgresSession::new();
    c.bench_function("expression_eval", |b| {
        b.iter(|| {
            let out = session.run_sync([FrontendMessage::Query {
                sql: "SELECT (1 + 2) * 3 + abs(-4) + sqrt(9)".to_string(),
            }]);
            assert_ok(&out);
        });
    });
}

fn bench_aggregate(c: &mut Criterion, name: &str, setup_sql: &str, query_sql: &str) {
    c.bench_function(name, |b| {
        let setup = setup_sql.to_string();
        let query = query_sql.to_string();
        b.iter_batched(
            || {
                let mut session = PostgresSession::new();
                let out = session.run_sync([FrontendMessage::Query { sql: setup.clone() }]);
                assert_ok(&out);
                session
            },
            |mut session| {
                let out = session.run_sync([FrontendMessage::Query { sql: query.clone() }]);
                assert_ok(&out);
            },
            BatchSize::SmallInput,
        );
    });
}

const AGG_1K_SETUP: &str =
    "CREATE TABLE bench_agg AS SELECT g AS val FROM generate_series(1, 1000) g";

fn bench_aggregate_sum_1k(c: &mut Criterion) {
    bench_aggregate(
        c,
        "aggregate_sum_1k",
        AGG_1K_SETUP,
        "SELECT sum(val) FROM bench_agg",
    );
}

fn bench_aggregate_min_1k(c: &mut Criterion) {
    bench_aggregate(
        c,
        "aggregate_min_1k",
        AGG_1K_SETUP,
        "SELECT min(val) FROM bench_agg",
    );
}

fn bench_aggregate_max_1k(c: &mut Criterion) {
    bench_aggregate(
        c,
        "aggregate_max_1k",
        AGG_1K_SETUP,
        "SELECT max(val) FROM bench_agg",
    );
}

fn bench_aggregate_group_by_1k(c: &mut Criterion) {
    bench_aggregate(
        c,
        "aggregate_group_by_1k",
        "CREATE TABLE bench_agg_gb AS SELECT g % 10 AS grp, g AS val FROM generate_series(1, 1000) g",
        "SELECT grp, sum(val), min(val), max(val) FROM bench_agg_gb GROUP BY grp",
    );
}

criterion_group!(
    benches,
    bench_simple_select,
    bench_insert_throughput,
    bench_join_performance,
    bench_expression_eval,
    bench_aggregate_sum_1k,
    bench_aggregate_min_1k,
    bench_aggregate_max_1k,
    bench_aggregate_group_by_1k
);
criterion_main!(benches);
