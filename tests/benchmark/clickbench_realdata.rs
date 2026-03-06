/// ClickBench benchmark with real data (100K rows from the full hits dataset).
///
/// Run with: cargo test --test benchmark clickbench_realdata -- --nocapture
///
/// Requires: data/clickbench/hits_subset.tsv (run scripts/load_clickbench.sh first)
use std::fs;
use std::path::Path;
use std::time::Instant;

use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

fn assert_ok(out: &[BackendMessage]) {
    for msg in out {
        if let BackendMessage::ErrorResponse { message, .. } = msg {
            panic!("query error: {message}");
        }
    }
}

fn exec(session: &mut PostgresSession, sql: &str) {
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert_ok(&out);
}

fn count_data_rows(out: &[BackendMessage]) -> usize {
    out.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
}

fn has_error(out: &[BackendMessage]) -> bool {
    out.iter()
        .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. }))
}

fn error_message(out: &[BackendMessage]) -> String {
    out.iter()
        .filter_map(|msg| {
            if let BackendMessage::ErrorResponse { message, .. } = msg {
                Some(message.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Load TSV data via batch INSERT statements (since COPY requires stdin pipe).
fn load_tsv_data(session: &mut PostgresSession, tsv_path: &Path, max_rows: usize) -> usize {
    let content = fs::read_to_string(tsv_path).expect("Failed to read TSV file");
    let mut loaded = 0;
    let batch_size = 100;
    let mut batch_values: Vec<String> = Vec::with_capacity(batch_size);

    for line in content.lines() {
        if loaded >= max_rows {
            break;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 105 {
            continue;
        }

        // Build a VALUES row, quoting text fields and handling empty strings
        let mut vals = Vec::with_capacity(105);
        // Column type map: TEXT columns by index (0-based)
        let text_cols: &[usize] = &[
            2, 13, 14, 25, 29, 34, 35, 39, 50, 56, 63, 75, 76, 77, 78, 84, 87, 88, 91, 92, 93, 94,
            95, 96, 97, 98, 99, 100,
        ];

        for (i, field) in fields.iter().enumerate().take(105) {
            if i == 5 {
                // EventDate — DATE column
                vals.push(format!("DATE '{}'", field.replace('\'', "''")));
            } else if text_cols.contains(&i) {
                // Escape single quotes and backslashes for SQL string literals
                let escaped = field
                    .replace('\\', "\\\\")
                    .replace('\'', "''")
                    .replace('\0', "");
                vals.push(format!("E'{}'", escaped));
            } else {
                // Numeric — handle empty as 0
                let trimmed = field.trim();
                if trimmed.is_empty() || trimmed.parse::<f64>().is_err() {
                    vals.push("0".to_string());
                } else {
                    vals.push(trimmed.to_string());
                }
            }
        }
        batch_values.push(format!("({})", vals.join(",")));
        loaded += 1;

        if batch_values.len() >= batch_size {
            let sql = format!("INSERT INTO hits VALUES {}", batch_values.join(","));
            exec(session, &sql);
            batch_values.clear();
        }
    }

    // Flush remaining
    if !batch_values.is_empty() {
        let sql = format!("INSERT INTO hits VALUES {}", batch_values.join(","));
        exec(session, &sql);
    }

    loaded
}

#[test]
fn clickbench_realdata_suite() {
    let _guard = super::benchmark_lock()
        .lock()
        .expect("benchmark lock should not be poisoned");
    let tsv_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("clickbench")
        .join("hits_subset.tsv");

    if !tsv_path.exists() {
        eprintln!("⚠ Skipping clickbench_realdata: data/clickbench/hits_subset.tsv not found");
        eprintln!("  Run: ./scripts/load_clickbench.sh");
        return;
    }

    let mut session = PostgresSession::new();

    // Create schema
    exec(&mut session, "DROP TABLE IF EXISTS hits");
    exec(&mut session, include_str!("clickbench_schema.sql"));

    // Load data — use 10K rows for a fast meaningful test
    let max_rows = std::env::var("CLICKBENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    eprintln!("\n=== ClickBench Real Data Benchmark ===");
    eprintln!("Loading {} rows...", max_rows);
    let load_start = Instant::now();
    let loaded = load_tsv_data(&mut session, &tsv_path, max_rows);
    let load_time = load_start.elapsed();
    eprintln!("Loaded {} rows in {:.2?}", loaded, load_time);
    eprintln!(
        "Load rate: {:.0} rows/sec\n",
        loaded as f64 / load_time.as_secs_f64()
    );

    // Verify row count
    let out = session.run_sync([FrontendMessage::Query {
        sql: "SELECT COUNT(*) FROM hits".to_string(),
    }]);
    assert_ok(&out);

    let queries: &[(&str, &str)] = &[
        ("Q00", "SELECT COUNT(*) FROM hits"),
        ("Q01", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"),
        (
            "Q02",
            "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
        ),
        ("Q03", "SELECT AVG(UserID) FROM hits"),
        ("Q04", "SELECT COUNT(DISTINCT UserID) FROM hits"),
        ("Q05", "SELECT COUNT(DISTINCT SearchPhrase) FROM hits"),
        ("Q06", "SELECT MIN(EventDate), MAX(EventDate) FROM hits"),
        (
            "Q07",
            "SELECT AdvEngineID, COUNT(*) AS c FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC",
        ),
        (
            "Q08",
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q09",
            "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q10",
            "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '0' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q11",
            "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '0' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q12",
            "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q13",
            "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q14",
            "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q15",
            "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q16",
            "SELECT UserID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q17",
            "SELECT UserID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
        ),
        (
            "Q18",
            "SELECT UserID, EventTime, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, EventTime, SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q19",
            "SELECT UserID FROM hits WHERE UserID = -6101065172494233192",
        ),
        ("Q20", "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'"),
        (
            "Q21",
            "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q22",
            "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q23",
            "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
        ),
        (
            "Q24",
            "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
        ),
        (
            "Q25",
            "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
        ),
        (
            "Q26",
            "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
        ),
        (
            "Q27",
            "SELECT CounterID, AVG(CAST(ResolutionWidth AS NUMERIC)) + 100 AS avg_rw FROM hits GROUP BY CounterID ORDER BY avg_rw DESC LIMIT 10",
        ),
        (
            "Q28",
            "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q29",
            "SELECT RegionID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY RegionID, SearchPhrase ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q30",
            "SELECT RegionID, CounterID, COUNT(*) AS c FROM hits GROUP BY RegionID, CounterID ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q31",
            "SELECT CounterID, RegionID, UserID, COUNT(*) AS c FROM hits GROUP BY CounterID, RegionID, UserID ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q32",
            "SELECT EventDate, RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY EventDate, RegionID ORDER BY EventDate, RegionID",
        ),
        (
            "Q33",
            "SELECT EventDate, RegionID, CounterID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY EventDate, RegionID, CounterID ORDER BY EventDate, RegionID, CounterID",
        ),
        (
            "Q34",
            "SELECT OS, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY OS ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q35",
            "SELECT UserAgent, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY UserAgent ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q36",
            "SELECT UserAgent, RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY UserAgent, RegionID ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q37",
            "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
        ),
        (
            "Q38",
            "SELECT CounterID, COUNT(*) AS cnt FROM hits GROUP BY CounterID ORDER BY cnt DESC LIMIT 20",
        ),
        (
            "Q39",
            "SELECT SearchPhrase, COUNT(*) AS cnt FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY cnt DESC LIMIT 20",
        ),
        (
            "Q40",
            "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN SearchEngineID = 0 AND AdvEngineID = 0 THEN Referer ELSE '' END AS src, URL, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, src, URL ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q41",
            "SELECT URLHash, EventDate, COUNT(*) AS c FROM hits GROUP BY URLHash, EventDate ORDER BY c DESC LIMIT 10",
        ),
        (
            "Q42",
            "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS c FROM hits WHERE WindowClientWidth > 0 AND WindowClientHeight > 0 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY c DESC LIMIT 10",
        ),
    ];

    let mut results: Vec<(&str, f64, usize, bool)> = Vec::new();
    let total_start = Instant::now();

    for (label, sql) in queries {
        let start = Instant::now();
        let out = session.run_sync([FrontendMessage::Query {
            sql: sql.to_string(),
        }]);
        let elapsed = start.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;
        let rows = count_data_rows(&out);
        let ok = !has_error(&out);

        if ok {
            eprintln!("  {label}: {ms:>8.2} ms  ({rows} rows)");
        } else {
            let err = error_message(&out);
            eprintln!("  {label}: FAIL — {err}");
        }
        results.push((label, ms, rows, ok));
    }

    let total_time = total_start.elapsed();
    let passed = results.iter().filter(|r| r.3).count();
    let total_queries = results.len();

    eprintln!("\n=== Summary ({} rows) ===", loaded);
    eprintln!("Passed: {passed}/{total_queries}");
    eprintln!("Total query time: {:.2?}", total_time);

    let query_times: Vec<f64> = results.iter().filter(|r| r.3).map(|r| r.1).collect();
    if !query_times.is_empty() {
        let sum: f64 = query_times.iter().sum();
        let min = query_times.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = query_times.iter().cloned().fold(0.0_f64, f64::max);
        let avg = sum / query_times.len() as f64;
        eprintln!("Query times: min={min:.2}ms avg={avg:.2}ms max={max:.2}ms total={sum:.2}ms");
    }

    // Print slow queries (> 2x average)
    let avg_ms: f64 = query_times.iter().sum::<f64>() / query_times.len() as f64;
    let slow: Vec<_> = results
        .iter()
        .filter(|r| r.3 && r.1 > avg_ms * 2.0)
        .collect();
    if !slow.is_empty() {
        eprintln!("\n🐌 Slow queries (> {:.0}ms):", avg_ms * 2.0);
        for (label, ms, rows, _) in &slow {
            eprintln!("  {label}: {ms:.2}ms ({rows} rows)");
        }
    }
}
