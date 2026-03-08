/// ClickBench large-dataset benchmark for OpenAssay.
///
/// Loads the 100K-row subset via COPY, then runs all 43 ClickBench queries
/// with timing. Outputs results as a table for analysis.
///
/// Usage:
///   cargo bench --bench clickbench_large
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn assert_ok(out: &[BackendMessage]) {
    for msg in out {
        if let BackendMessage::ErrorResponse { message, code, .. } = msg {
            panic!("query produced error [{code}]: {message}");
        }
    }
}

fn count_data_rows(out: &[BackendMessage]) -> usize {
    out.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
}

fn exec(session: &mut PostgresSession, sql: &str) -> Vec<BackendMessage> {
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert_ok(&out);
    out
}

// ---------------------------------------------------------------------------
// Schema (same as clickbench_bench.rs)
// ---------------------------------------------------------------------------

// Official ClickBench PostgreSQL schema (matches the TSV data format)
const CLICKBENCH_SCHEMA: &str = r"
CREATE TABLE hits (
    WatchID              BIGINT NOT NULL,
    JavaEnable           SMALLINT NOT NULL,
    Title                TEXT NOT NULL,
    GoodEvent            SMALLINT NOT NULL,
    EventTime            TIMESTAMP NOT NULL,
    EventDate            DATE NOT NULL,
    CounterID            INTEGER NOT NULL,
    ClientIP             INTEGER NOT NULL,
    RegionID             INTEGER NOT NULL,
    UserID               BIGINT NOT NULL,
    CounterClass         SMALLINT NOT NULL,
    OS                   SMALLINT NOT NULL,
    UserAgent            SMALLINT NOT NULL,
    URL                  TEXT NOT NULL,
    Referer              TEXT NOT NULL,
    IsRefresh            SMALLINT NOT NULL,
    RefererCategoryID    SMALLINT NOT NULL,
    RefererRegionID      INTEGER NOT NULL,
    URLCategoryID        SMALLINT NOT NULL,
    URLRegionID          INTEGER NOT NULL,
    ResolutionWidth      SMALLINT NOT NULL,
    ResolutionHeight     SMALLINT NOT NULL,
    ResolutionDepth      SMALLINT NOT NULL,
    FlashMajor           SMALLINT NOT NULL,
    FlashMinor           SMALLINT NOT NULL,
    FlashMinor2          TEXT NOT NULL,
    NetMajor             SMALLINT NOT NULL,
    NetMinor             SMALLINT NOT NULL,
    UserAgentMajor       SMALLINT NOT NULL,
    UserAgentMinor       VARCHAR(255) NOT NULL,
    CookieEnable         SMALLINT NOT NULL,
    JavascriptEnable     SMALLINT NOT NULL,
    IsMobile             SMALLINT NOT NULL,
    MobilePhone          SMALLINT NOT NULL,
    MobilePhoneModel     TEXT NOT NULL,
    Params               TEXT NOT NULL,
    IPNetworkID          INTEGER NOT NULL,
    TraficSourceID       SMALLINT NOT NULL,
    SearchEngineID       SMALLINT NOT NULL,
    SearchPhrase         TEXT NOT NULL,
    AdvEngineID          SMALLINT NOT NULL,
    IsArtifical          SMALLINT NOT NULL,
    WindowClientWidth    SMALLINT NOT NULL,
    WindowClientHeight   SMALLINT NOT NULL,
    ClientTimeZone       SMALLINT NOT NULL,
    ClientEventTime      TIMESTAMP NOT NULL,
    SilverlightVersion1  SMALLINT NOT NULL,
    SilverlightVersion2  SMALLINT NOT NULL,
    SilverlightVersion3  INTEGER NOT NULL,
    SilverlightVersion4  SMALLINT NOT NULL,
    PageCharset          TEXT NOT NULL,
    CodeVersion          INTEGER NOT NULL,
    IsLink               SMALLINT NOT NULL,
    IsDownload           SMALLINT NOT NULL,
    IsNotBounce          SMALLINT NOT NULL,
    FUniqID              BIGINT NOT NULL,
    OriginalURL          TEXT NOT NULL,
    HID                  INTEGER NOT NULL,
    IsOldCounter         SMALLINT NOT NULL,
    IsEvent              SMALLINT NOT NULL,
    IsParameter          SMALLINT NOT NULL,
    DontCountHits        SMALLINT NOT NULL,
    WithHash             SMALLINT NOT NULL,
    HitColor             CHAR NOT NULL,
    LocalEventTime       TIMESTAMP NOT NULL,
    Age                  SMALLINT NOT NULL,
    Sex                  SMALLINT NOT NULL,
    Income               SMALLINT NOT NULL,
    Interests            SMALLINT NOT NULL,
    Robotness            SMALLINT NOT NULL,
    RemoteIP             INTEGER NOT NULL,
    WindowName           INTEGER NOT NULL,
    OpenerName           INTEGER NOT NULL,
    HistoryLength        SMALLINT NOT NULL,
    BrowserLanguage      TEXT NOT NULL,
    BrowserCountry       TEXT NOT NULL,
    SocialNetwork        TEXT NOT NULL,
    SocialAction         TEXT NOT NULL,
    HTTPError            SMALLINT NOT NULL,
    SendTiming           INTEGER NOT NULL,
    DNSTiming            INTEGER NOT NULL,
    ConnectTiming        INTEGER NOT NULL,
    ResponseStartTiming  INTEGER NOT NULL,
    ResponseEndTiming    INTEGER NOT NULL,
    FetchTiming          INTEGER NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage     TEXT NOT NULL,
    ParamPrice           BIGINT NOT NULL,
    ParamOrderID         TEXT NOT NULL,
    ParamCurrency        TEXT NOT NULL,
    ParamCurrencyID      SMALLINT NOT NULL,
    OpenstatServiceName  TEXT NOT NULL,
    OpenstatCampaignID   TEXT NOT NULL,
    OpenstatAdID         TEXT NOT NULL,
    OpenstatSourceID     TEXT NOT NULL,
    UTMSource            TEXT NOT NULL,
    UTMMedium            TEXT NOT NULL,
    UTMCampaign          TEXT NOT NULL,
    UTMContent           TEXT NOT NULL,
    UTMTerm              TEXT NOT NULL,
    FromTag              TEXT NOT NULL,
    HasGCLID             SMALLINT NOT NULL,
    RefererHash          BIGINT NOT NULL,
    URLHash              BIGINT NOT NULL,
    CLID                 INTEGER NOT NULL
);
";

// ---------------------------------------------------------------------------
// ClickBench queries (all 43)
// ---------------------------------------------------------------------------

const CLICKBENCH_QUERIES: [(&str, &str); 43] = [
    ("q00", "SELECT COUNT(*) FROM hits"),
    ("q01", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"),
    (
        "q02",
        "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    ),
    ("q03", "SELECT AVG(UserID) FROM hits"),
    ("q04", "SELECT COUNT(DISTINCT UserID) FROM hits"),
    ("q05", "SELECT COUNT(DISTINCT SearchPhrase) FROM hits"),
    ("q06", "SELECT MIN(EventDate), MAX(EventDate) FROM hits"),
    (
        "q07",
        "SELECT AdvEngineID, COUNT(*) AS c FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY c DESC",
    ),
    (
        "q08",
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    ),
    (
        "q09",
        "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    ),
    (
        "q10",
        "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
    ),
    (
        "q11",
        "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
    ),
    (
        "q12",
        "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "q13",
        "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
    ),
    (
        "q14",
        "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "q15",
        "SELECT UserID, COUNT(*) AS c FROM hits GROUP BY UserID ORDER BY c DESC LIMIT 10",
    ),
    (
        "q16",
        "SELECT UserID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "q17",
        "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10",
    ),
    (
        "q18",
        "SELECT UserID, EventTime, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY UserID, EventTime, SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    ("q19", "SELECT UserID FROM hits WHERE UserID = 1001"),
    (
        "q20",
        "SELECT COUNT(*) FROM hits WHERE URL LIKE '%example%'",
    ),
    (
        "q21",
        "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%example%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "q22",
        "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Analytics%' AND URL NOT LIKE '%.internal%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "q23",
        "SELECT * FROM hits WHERE URL LIKE '%widget%' ORDER BY EventTime LIMIT 10",
    ),
    (
        "q24",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
    ),
    (
        "q25",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
    ),
    (
        "q26",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    ),
    (
        "q27",
        "SELECT CounterID, AVG(CAST(ResolutionWidth AS NUMERIC)) + 100 AS avg_rw FROM hits GROUP BY CounterID ORDER BY avg_rw DESC LIMIT 10",
    ),
    (
        "q28",
        "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10",
    ),
    (
        "q29",
        "SELECT RegionID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY RegionID, SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    (
        "q30",
        "SELECT RegionID, CounterID, COUNT(*) AS c FROM hits GROUP BY RegionID, CounterID ORDER BY c DESC LIMIT 10",
    ),
    (
        "q31",
        "SELECT CounterID, RegionID, UserID, COUNT(*) AS c FROM hits GROUP BY CounterID, RegionID, UserID ORDER BY c DESC LIMIT 10",
    ),
    (
        "q32",
        "SELECT EventDate, RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY EventDate, RegionID ORDER BY EventDate, RegionID",
    ),
    (
        "q33",
        "SELECT EventDate, RegionID, CounterID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY EventDate, RegionID, CounterID ORDER BY EventDate, RegionID, CounterID",
    ),
    (
        "q34",
        "SELECT OS, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY OS ORDER BY u DESC LIMIT 10",
    ),
    (
        "q35",
        "SELECT UserAgent, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY UserAgent ORDER BY u DESC LIMIT 10",
    ),
    (
        "q36",
        "SELECT UserAgent, RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY UserAgent, RegionID ORDER BY u DESC LIMIT 10",
    ),
    (
        "q37",
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    ),
    (
        "q38",
        "SELECT CounterID, COUNT(*) AS cnt FROM hits GROUP BY CounterID ORDER BY cnt DESC LIMIT 20",
    ),
    (
        "q39",
        "SELECT SearchPhrase, COUNT(*) AS cnt FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY cnt DESC LIMIT 20",
    ),
    (
        "q40",
        "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN SearchEngineID = 0 AND AdvEngineID = 0 THEN Referer ELSE '' END AS src, URL, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, src, URL ORDER BY c DESC LIMIT 10",
    ),
    (
        "q41",
        "SELECT URLHash, EventDate, COUNT(*) AS c FROM hits GROUP BY URLHash, EventDate ORDER BY c DESC LIMIT 10",
    ),
    (
        "q42",
        "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS c FROM hits WHERE WindowClientWidth > 0 AND WindowClientHeight > 0 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY c DESC LIMIT 10",
    ),
];

// ---------------------------------------------------------------------------
// Load data via COPY
// ---------------------------------------------------------------------------

fn load_clickbench_data(session: &mut PostgresSession) {
    let data_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/data/clickbench/hits_subset.tsv"
    );
    let data =
        std::fs::read(data_path).unwrap_or_else(|e| panic!("Failed to read {data_path}: {e}"));

    // Start COPY (default delimiter is tab)
    let out = session.run_sync([FrontendMessage::Query {
        sql: "COPY hits FROM STDIN".to_string(),
    }]);
    // Should get CopyInResponse
    let got_copy_in = out
        .iter()
        .any(|m| matches!(m, BackendMessage::CopyInResponse { .. }));
    assert!(got_copy_in, "Expected CopyInResponse, got: {out:?}");

    // Send data + done
    let out = session.run_sync([
        FrontendMessage::CopyData { data },
        FrontendMessage::CopyDone,
    ]);
    assert_ok(&out);
}

fn create_loaded_session() -> PostgresSession {
    let start = Instant::now();
    let mut session = PostgresSession::new();
    exec(&mut session, "DROP TABLE IF EXISTS hits");
    exec(&mut session, CLICKBENCH_SCHEMA);

    eprintln!("Loading 100K rows via COPY...");
    load_clickbench_data(&mut session);

    // Verify row count
    let out = exec(&mut session, "SELECT COUNT(*) FROM hits");
    let rows = count_data_rows(&out);
    eprintln!(
        "Load complete in {:.1}s ({rows} result rows)",
        start.elapsed().as_secs_f64()
    );

    session
}

// ---------------------------------------------------------------------------
// Criterion benchmark
// ---------------------------------------------------------------------------

fn bench_clickbench_100k(c: &mut Criterion) {
    // Load data once
    let mut session = create_loaded_session();

    // Warm-up: run each query once
    eprintln!("\n=== Warm-up run ===");
    for (label, sql) in &CLICKBENCH_QUERIES {
        let start = Instant::now();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let out = session.run_sync([FrontendMessage::Query {
                sql: (*sql).to_string(),
            }]);
            out
        }));
        match result {
            Ok(out) => {
                let has_error = out
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
                let rows = count_data_rows(&out);
                let elapsed = start.elapsed();
                if has_error {
                    let err_msg = out
                        .iter()
                        .find_map(|m| {
                            if let BackendMessage::ErrorResponse { message, .. } = m {
                                Some(message.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();
                    eprintln!("{label}: ERROR ({err_msg}) [{elapsed:.3?}]");
                } else {
                    eprintln!("{label}: {rows} rows [{elapsed:.3?}]");
                }
            }
            Err(_) => {
                eprintln!("{label}: PANIC");
            }
        }
    }

    // Benchmark each query
    let mut group = c.benchmark_group("clickbench_100k");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_millis(100));
    group.measurement_time(std::time::Duration::from_secs(10));

    for (label, sql) in &CLICKBENCH_QUERIES {
        group.bench_with_input(BenchmarkId::new("query", label), sql, |b, sql| {
            b.iter(|| {
                let out = session.run_sync([FrontendMessage::Query {
                    sql: (*sql).to_string(),
                }]);
                let _ = count_data_rows(&out);
            });
        });
    }
    group.finish();
}

criterion_group!(clickbench_100k_benches, bench_clickbench_100k,);
criterion_main!(clickbench_100k_benches);
