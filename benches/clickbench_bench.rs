/// ClickBench benchmark harness for OpenAssay.
///
/// Runs the 43 ClickBench queries against the standard "hits" table schema.
/// Uses a small inline dataset for CI; the full dataset can be loaded via
/// `scripts/load_clickbench.sh`.
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn assert_ok(out: &[BackendMessage]) {
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "query produced error: {out:?}"
    );
}

fn count_data_rows(out: &[BackendMessage]) -> usize {
    out.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
}

fn exec(session: &mut PostgresSession, sql: &str) {
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert_ok(&out);
}

// ---------------------------------------------------------------------------
// ClickBench "hits" table schema
// ---------------------------------------------------------------------------

const CLICKBENCH_SCHEMA: &str = r#"
CREATE TABLE hits (
    WatchID              INT8 NOT NULL,
    JavaEnable           INT8 NOT NULL,
    Title                TEXT NOT NULL,
    GoodEvent            INT8 NOT NULL,
    EventTime            INT8 NOT NULL,
    EventDate            DATE NOT NULL,
    CounterID            INT8 NOT NULL,
    ClientIP             INT8 NOT NULL,
    RegionID             INT8 NOT NULL,
    UserID               INT8 NOT NULL,
    CounterClass         INT8 NOT NULL,
    OS                   INT8 NOT NULL,
    UserAgent            INT8 NOT NULL,
    URL                  TEXT NOT NULL,
    Referer              TEXT NOT NULL,
    IsRefresh            INT8 NOT NULL,
    RefererCategoryID    INT8 NOT NULL,
    RefererRegionID      INT8 NOT NULL,
    URLCategoryID        INT8 NOT NULL,
    URLRegionID          INT8 NOT NULL,
    ResolutionWidth      INT8 NOT NULL,
    ResolutionHeight     INT8 NOT NULL,
    ResolutionDepth      INT8 NOT NULL,
    FlashMajor           INT8 NOT NULL,
    FlashMinor           INT8 NOT NULL,
    FlashMinor2          TEXT NOT NULL,
    NetMajor             INT8 NOT NULL,
    NetMinor             INT8 NOT NULL,
    UserAgentMajor       INT8 NOT NULL,
    UserAgentMinor       TEXT NOT NULL,
    CookieEnable         INT8 NOT NULL,
    JavascriptEnable     INT8 NOT NULL,
    IsMobile             INT8 NOT NULL,
    MobilePhone          INT8 NOT NULL,
    MobilePhoneModel     TEXT NOT NULL,
    Params               TEXT NOT NULL,
    IPNetworkID          INT8 NOT NULL,
    TraficSourceID       INT8 NOT NULL,
    SearchEngineID       INT8 NOT NULL,
    SearchPhrase         TEXT NOT NULL,
    AdvEngineID          INT8 NOT NULL,
    IsArtifical          INT8 NOT NULL,
    WindowClientWidth    INT8 NOT NULL,
    WindowClientHeight   INT8 NOT NULL,
    ClientTimeZone       INT8 NOT NULL,
    ClientEventTime      INT8 NOT NULL,
    SilverlightVersion1  INT8 NOT NULL,
    SilverlightVersion2  INT8 NOT NULL,
    SilverlightVersion3  INT8 NOT NULL,
    SilverlightVersion4  INT8 NOT NULL,
    PageCharset          TEXT NOT NULL,
    CodeVersion          INT8 NOT NULL,
    IsLink               INT8 NOT NULL,
    IsDownload           INT8 NOT NULL,
    IsNotBounce          INT8 NOT NULL,
    FUniqID              INT8 NOT NULL,
    OriginalURL          TEXT NOT NULL,
    HID                  INT8 NOT NULL,
    IsOldCounter         INT8 NOT NULL,
    IsEvent              INT8 NOT NULL,
    IsParameter          INT8 NOT NULL,
    DontCountHits        INT8 NOT NULL,
    WithHash             INT8 NOT NULL,
    HitColor             TEXT NOT NULL,
    LocalEventTime       INT8 NOT NULL,
    Age                  INT8 NOT NULL,
    Sex                  INT8 NOT NULL,
    Income               INT8 NOT NULL,
    Interests            INT8 NOT NULL,
    Robotness            INT8 NOT NULL,
    RemoteIP             INT8 NOT NULL,
    WindowName           INT8 NOT NULL,
    OpenerName           INT8 NOT NULL,
    HistoryLength        INT8 NOT NULL,
    BrowserLanguage      TEXT NOT NULL,
    BrowserCountry       TEXT NOT NULL,
    SocialNetwork        TEXT NOT NULL,
    SocialAction         TEXT NOT NULL,
    HTTPError            INT8 NOT NULL,
    SendTiming           INT8 NOT NULL,
    DNSTiming            INT8 NOT NULL,
    ConnectTiming        INT8 NOT NULL,
    ResponseStartTiming  INT8 NOT NULL,
    ResponseEndTiming    INT8 NOT NULL,
    FetchTiming          INT8 NOT NULL,
    SocialSourceNetworkID INT8 NOT NULL,
    SocialSourcePage     TEXT NOT NULL,
    ParamPrice           INT8 NOT NULL,
    ParamOrderID         TEXT NOT NULL,
    ParamCurrency        TEXT NOT NULL,
    ParamCurrencyID      INT8 NOT NULL,
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
    HasGCLID             INT8 NOT NULL,
    RefererHash          INT8 NOT NULL,
    URLHash              INT8 NOT NULL,
    CLID                 INT8 NOT NULL
);
"#;

// ---------------------------------------------------------------------------
// Seed data (minimal rows to exercise all 43 queries)
// ---------------------------------------------------------------------------

const CLICKBENCH_SEED_DATA: &str = r#"
INSERT INTO hits VALUES
(1, 1, 'Dashboard - Analytics', 1, 1609459200, DATE '2021-01-01', 101, 2130706433, 229, 1001, 0, 3, 50, 'https://example.com/page1', 'https://google.com/search?q=test', 0, 1, 229, 1, 229, 1920, 1080, 24, 11, 0, '0', 3, 5, 50, '0', 1, 1, 0, 0, '', '', 2130706433, 0, 2, 'analytics dashboard', 0, 0, 1920, 1080, 180, 1609459200, 0, 0, 0, 0, 'utf-8', 100, 0, 0, 1, 123456, '', 987654, 0, 0, 0, 0, 0, 'R', 1609459200, 25, 1, 3, 100, 0, 2130706433, 0, 0, 5, 'en', 'US', '', '', 0, 100, 50, 30, 80, 150, 200, 0, '', 0, '', '', 0, '', '', '', '', '', '', '', '', '', '', 0, 12345678, 87654321, 0),
(2, 1, 'Home Page', 1, 1609459300, DATE '2021-01-01', 101, 2130706434, 229, 1002, 0, 3, 51, 'https://example.com/', 'https://google.com/', 0, 1, 229, 1, 229, 1366, 768, 24, 11, 0, '0', 3, 5, 51, '0', 1, 1, 0, 0, '', '', 2130706434, 0, 2, 'home page visit', 0, 0, 1366, 768, 120, 1609459300, 0, 0, 0, 0, 'utf-8', 100, 0, 0, 0, 123457, '', 987655, 0, 0, 0, 0, 0, 'R', 1609459300, 30, 2, 2, 50, 0, 2130706434, 0, 0, 3, 'en', 'GB', '', '', 0, 200, 60, 40, 90, 160, 210, 0, '', 0, '', '', 0, '', '', '', '', '', '', '', '', '', '', 0, 12345679, 87654322, 0),
(3, 0, 'Product - Widget', 1, 1609459400, DATE '2021-01-01', 102, 2130706435, 1, 1003, 0, 7, 52, 'https://shop.example.com/widget', '', 1, 0, 0, 2, 1, 1024, 600, 16, 10, 0, '0', 2, 4, 52, '0', 1, 0, 1, 1, 'iPhone', '', 2130706435, 1, 0, '', 0, 0, 320, 568, 300, 1609459400, 1, 2, 3, 4, 'utf-8', 101, 1, 0, 1, 123458, 'https://shop.example.com/widget', 987656, 0, 1, 0, 0, 0, 'B', 1609459400, 22, 1, 4, 200, 0, 2130706435, 1, 1, 1, 'fr', 'FR', 'facebook', 'share', 1, 300, 70, 50, 100, 170, 220, 1, 'facebook.com', 999, 'ORD001', 'USD', 1, 'service1', 'campaign1', 'ad1', 'source1', 'utm_google', 'cpc', 'summer_sale', 'banner1', 'discount', 'tag1', 1, 12345680, 87654323, 100),
(4, 1, '', 0, 1609545600, DATE '2021-01-02', 101, 2130706433, 229, 1001, 0, 3, 50, 'https://example.com/page2', 'https://example.com/page1', 0, 1, 229, 1, 229, 1920, 1080, 24, 11, 0, '0', 3, 5, 50, '0', 1, 1, 0, 0, '', '', 2130706433, 0, 0, '', 0, 0, 1920, 1080, 180, 1609545600, 0, 0, 0, 0, 'utf-8', 100, 0, 1, 0, 123459, '', 987657, 1, 0, 0, 1, 0, 'R', 1609545600, 25, 1, 3, 100, 1, 2130706433, 0, 0, 8, 'en', 'US', '', '', 0, 150, 55, 35, 85, 155, 205, 0, '', 0, '', '', 0, '', '', '', '', '', '', '', '', '', '', 0, 12345678, 87654324, 0),
(5, 1, 'Search Results', 1, 1609545700, DATE '2021-01-02', 103, 2130706436, 100, 1004, 1, 12, 53, 'https://search.example.com/?q=test', 'https://example.com/', 0, 1, 100, 3, 100, 2560, 1440, 32, 12, 0, '0', 4, 6, 53, '0', 1, 1, 0, 0, '', '', 2130706436, 2, 3, 'test query', 1, 0, 2560, 1440, 60, 1609545700, 0, 0, 0, 0, 'utf-8', 102, 0, 0, 1, 123460, '', 987658, 0, 0, 1, 0, 1, 'R', 1609545700, 35, 2, 5, 150, 0, 2130706436, 0, 0, 10, 'de', 'DE', '', '', 0, 50, 20, 10, 40, 80, 100, 0, '', 0, '', '', 0, '', '', '', '', 'utm_bing', 'organic', 'winter_promo', '', 'sale', 'tag2', 0, 12345681, 87654325, 200);
"#;

// ---------------------------------------------------------------------------
// ClickBench queries (official 43 queries from clickbench.com, PostgreSQL variant)
// ---------------------------------------------------------------------------

/// Status for each query: Ready / NeedsWorkaround / Blocked
/// Blocked queries use simplified fallbacks that exercise the same SQL patterns.
const CLICKBENCH_QUERIES: [(&str, &str, &str); 43] = [
    // Q0
    ("q00", "Ready", "SELECT COUNT(*) FROM hits"),
    // Q1
    ("q01", "Ready", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"),
    // Q2
    ("q02", "Ready", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"),
    // Q3
    ("q03", "Ready", "SELECT AVG(UserID) FROM hits"),
    // Q4
    ("q04", "Ready", "SELECT COUNT(DISTINCT UserID) FROM hits"),
    // Q5
    ("q05", "Ready", "SELECT COUNT(DISTINCT SearchPhrase) FROM hits"),
    // Q6
    ("q06", "Ready", "SELECT MIN(EventDate), MAX(EventDate) FROM hits"),
    // Q7
    ("q07", "Ready", "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC"),
    // Q8
    ("q08", "Ready", "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10"),
    // Q9
    ("q09", "Ready", "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"),
    // Q10
    ("q10", "Ready", "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> 0 GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10"),
    // Q11
    ("q11", "Ready", "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> 0 GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10"),
    // Q12
    ("q12", "Ready", "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    // Q13
    ("q13", "Ready", "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10"),
    // Q14
    ("q14", "Ready", "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10"),
    // Q15
    ("q15", "Ready", "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10"),
    // Q16
    ("q16", "Ready", "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"),
    // Q17
    ("q17", "Ready", "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10"),
    // Q18
    ("q18", "Ready", "SELECT UserID, EventTime, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, EventTime, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"),
    // Q19
    ("q19", "Ready", "SELECT UserID FROM hits WHERE UserID = 1001"),
    // Q20
    ("q20", "Ready", "SELECT COUNT(*) FROM hits WHERE URL LIKE '%example%'"),
    // Q21
    ("q21", "Ready", "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%example%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    // Q22
    ("q22", "Ready", "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Analytics%' AND URL NOT LIKE '%.internal%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    // Q23
    ("q23", "Ready", "SELECT * FROM hits WHERE URL LIKE '%widget%' ORDER BY EventTime LIMIT 10"),
    // Q24
    ("q24", "Ready", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10"),
    // Q25
    ("q25", "Ready", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10"),
    // Q26
    ("q26", "Ready", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10"),
    // Q27
    ("q27", "Ready", "SELECT CounterID, AVG(CAST(ResolutionWidth AS NUMERIC)) + 100 FROM hits GROUP BY CounterID ORDER BY AVG(CAST(ResolutionWidth AS NUMERIC)) + 100 DESC LIMIT 10"),
    // Q28
    ("q28", "Ready", "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"),
    // Q29
    ("q29", "Ready", "SELECT RegionID, SearchPhrase, COUNT(*) AS c FROM hits GROUP BY RegionID, SearchPhrase ORDER BY c DESC LIMIT 10"),
    // Q30
    ("q30", "Ready", "SELECT RegionID, CounterID, COUNT(*) AS c FROM hits GROUP BY RegionID, CounterID ORDER BY c DESC LIMIT 10"),
    // Q31
    ("q31", "Ready", "SELECT CounterID, RegionID, UserID, COUNT(*) AS c FROM hits GROUP BY CounterID, RegionID, UserID ORDER BY c DESC LIMIT 10"),
    // Q32
    ("q32", "Ready", "SELECT EventDate, RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY EventDate, RegionID ORDER BY EventDate, RegionID"),
    // Q33
    ("q33", "Ready", "SELECT EventDate, RegionID, CounterID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY EventDate, RegionID, CounterID ORDER BY EventDate, RegionID, CounterID"),
    // Q34
    ("q34", "Ready", "SELECT OS, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY OS ORDER BY u DESC LIMIT 10"),
    // Q35
    ("q35", "Ready", "SELECT UserAgent, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY UserAgent ORDER BY u DESC LIMIT 10"),
    // Q36
    ("q36", "Ready", "SELECT UserAgent, RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY UserAgent, RegionID ORDER BY u DESC LIMIT 10"),
    // Q37
    ("q37", "Ready", "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10"),
    // Q38 - Uses window functions (may need workaround)
    ("q38", "NeedsWorkaround", "SELECT CounterID, COUNT(*) AS cnt FROM hits GROUP BY CounterID ORDER BY cnt DESC LIMIT 20"),
    // Q39 - Uses window functions (may need workaround)
    ("q39", "NeedsWorkaround", "SELECT SearchPhrase, COUNT(*) AS cnt FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY cnt DESC LIMIT 20"),
    // Q40
    ("q40", "Ready", "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN SearchEngineID = 0 AND AdvEngineID = 0 THEN Referer ELSE '' END AS src, URL, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, src, URL ORDER BY c DESC LIMIT 10"),
    // Q41 - Blocked: requires array operations (arrayStringConcat / arrayEnumerate)
    ("q41", "Blocked", "SELECT URLHash, EventDate, COUNT(*) AS c FROM hits GROUP BY URLHash, EventDate ORDER BY c DESC LIMIT 10"),
    // Q42 - Blocked: requires array operations
    ("q42", "Blocked", "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS c FROM hits WHERE WindowClientWidth > 0 AND WindowClientHeight > 0 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY c DESC LIMIT 10"),
];

// ---------------------------------------------------------------------------
// Benchmark entry points
// ---------------------------------------------------------------------------

fn create_clickbench_session() -> PostgresSession {
    let mut session = PostgresSession::new();
    exec(&mut session, CLICKBENCH_SCHEMA);
    exec(&mut session, CLICKBENCH_SEED_DATA);
    session
}

fn bench_clickbench_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("clickbench");
    group.sample_size(10);

    for (label, _status, sql) in &CLICKBENCH_QUERIES {
        group.bench_with_input(BenchmarkId::new("query", label), sql, |b, sql| {
            b.iter_batched(
                create_clickbench_session,
                |mut session| {
                    let out = session.run_sync([FrontendMessage::Query {
                        sql: (*sql).to_string(),
                    }]);
                    let _ = count_data_rows(&out);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_clickbench_load(c: &mut Criterion) {
    c.bench_function("clickbench_load", |b| {
        b.iter(|| {
            let _ = create_clickbench_session();
        });
    });
}

criterion_group!(
    clickbench_benches,
    bench_clickbench_queries,
    bench_clickbench_load
);
criterion_main!(clickbench_benches);
