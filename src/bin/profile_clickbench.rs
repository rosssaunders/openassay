use std::env;
use std::time::Instant;

use openassay::executor::profiling;
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

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

const QUERIES: [(&str, &str); 4] = [
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
];

fn assert_ok(out: &[BackendMessage]) {
    for msg in out {
        if let BackendMessage::ErrorResponse { message, code, .. } = msg {
            panic!("query produced error [{code}]: {message}");
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

fn load_clickbench_data(session: &mut PostgresSession) {
    let data_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/data/clickbench/hits_subset.tsv"
    );
    let data = std::fs::read(data_path).unwrap_or_else(|error| {
        panic!("Failed to read {data_path}: {error}");
    });

    let out = session.run_sync([FrontendMessage::Query {
        sql: "COPY hits FROM STDIN".to_string(),
    }]);
    let got_copy_in = out
        .iter()
        .any(|message| matches!(message, BackendMessage::CopyInResponse { .. }));
    assert!(got_copy_in, "Expected CopyInResponse, got: {out:?}");

    let out = session.run_sync([
        FrontendMessage::CopyData { data },
        FrontendMessage::CopyDone,
    ]);
    assert_ok(&out);
}

fn main() {
    let mut args = env::args().skip(1);
    let label = args.next().unwrap_or_else(|| "q20".to_string());
    let iterations = args
        .next()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(50);
    let sql = if let Some(raw_sql) = label.strip_prefix("sql:") {
        raw_sql.to_string()
    } else {
        QUERIES
            .iter()
            .find_map(|(query_label, query_sql)| (*query_label == label).then_some(*query_sql))
            .unwrap_or_else(|| panic!("unknown query label: {label}"))
            .to_string()
    };

    let total_start = Instant::now();
    let mut session = PostgresSession::new();
    exec(&mut session, "DROP TABLE IF EXISTS hits");
    exec(&mut session, CLICKBENCH_SCHEMA);
    load_clickbench_data(&mut session);

    profiling::reset();
    eprintln!("ready query={label} iterations={iterations}");

    let query_start = Instant::now();
    let mut last_row_count = 0usize;
    for _ in 0..iterations {
        let out = session.run_sync([FrontendMessage::Query { sql: sql.clone() }]);
        assert_ok(&out);
        last_row_count = count_data_rows(&out);
    }
    let query_elapsed = query_start.elapsed();
    let total_elapsed = total_start.elapsed();

    eprintln!(
        "done query={label} iterations={iterations} rows={last_row_count} query_secs={:.3} total_secs={:.3}",
        query_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64()
    );
    println!("{}", profiling::report());
}
