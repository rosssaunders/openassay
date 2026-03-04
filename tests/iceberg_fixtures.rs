#![cfg(not(target_arch = "wasm32"))]
#![allow(dead_code)]

use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::writer::SerializedFileWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::parser::parse_message_type;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub type FixtureResult<T> = Result<T, Box<dyn std::error::Error>>;

const BASE_TIMESTAMP_MS: i64 = 1_700_000_000_000; // ~Nov 2023

pub struct IcebergFixtureSet {
    pub root: PathBuf,
    pub deribit_trades: PathBuf,
    pub deribit_ohlcv: PathBuf,
    pub deribit_options: PathBuf,
    pub multi_schema: PathBuf,
}

impl IcebergFixtureSet {
    pub fn create() -> FixtureResult<Self> {
        let root = std::env::temp_dir().join(format!("openassay-iceberg-{}", std::process::id()));
        if root.exists() {
            fs::remove_dir_all(&root)?;
        }
        fs::create_dir_all(&root)?;

        let deribit_trades = root.join("deribit_trades");
        let deribit_ohlcv = root.join("deribit_ohlcv");
        let deribit_options = root.join("deribit_options");
        let multi_schema = root.join("multi_schema");

        create_trades_table(&deribit_trades, 120, 2)?;
        create_ohlcv_table(&deribit_ohlcv, 60)?;
        create_options_table(&deribit_options, 40)?;
        create_multi_schema_table(&multi_schema)?;

        Ok(Self {
            root,
            deribit_trades,
            deribit_ohlcv,
            deribit_options,
            multi_schema,
        })
    }

    pub fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl Drop for IcebergFixtureSet {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn create_table_dirs(table_root: &Path) -> FixtureResult<(PathBuf, PathBuf)> {
    let metadata_dir = table_root.join("metadata");
    let data_dir = table_root.join("data");
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;
    Ok((metadata_dir, data_dir))
}

// ── Trade rows ──

struct TradeRow {
    timestamp: i64,
    instrument: String,
    price: f64,
    amount: f64,
    direction: String,
    iv: Option<f64>,
    index_price: f64,
    trade_id: String,
}

fn generate_trade_rows(count: usize) -> Vec<TradeRow> {
    (0..count)
        .map(|idx| {
            let is_btc = idx % 2 == 0;
            let timestamp = BASE_TIMESTAMP_MS + i64::try_from(idx).unwrap_or(0) * 60_000;
            let direction = if idx % 3 == 0 { "sell" } else { "buy" };

            let (price, amount, iv, index_price, trade_id) = if is_btc {
                let price = 60_000.0
                    + f64::from((idx * 37 % 5_000) as u32)
                    + f64::from((idx % 11) as u32) * 0.17;
                let amount = 0.1 + f64::from((idx * 17 % 100) as u32) / 10.0;
                let iv = if idx % 11 == 0 { None } else { Some(0.5 + f64::from((idx * 7 % 50) as u32) / 100.0) };
                let index_price = 60_000.0 + f64::from((idx * 41 % 4_000) as u32);
                let trade_id = format!("BTC-PERPETUAL-{}", 12_345 + idx);
                (price, amount, iv, index_price, trade_id)
            } else {
                let price = 3_200.0 + f64::from((idx * 13 % 300) as u32) + f64::from((idx % 7) as u32) * 0.09;
                let amount = 1.0 + f64::from((idx * 23 % 990) as u32) / 10.0;
                let iv = if idx % 7 == 0 { None } else { Some(0.4 + f64::from((idx * 11 % 60) as u32) / 100.0) };
                let index_price = 3_200.0 + f64::from((idx * 19 % 200) as u32);
                let trade_id = format!("ETH-PERPETUAL-{}", 67_890 + idx);
                (price, amount, iv, index_price, trade_id)
            };

            TradeRow {
                timestamp,
                instrument: if is_btc { "BTC-PERPETUAL".to_string() } else { "ETH-PERPETUAL".to_string() },
                price,
                amount,
                direction: direction.to_string(),
                iv,
                index_price,
                trade_id,
            }
        })
        .collect()
}

fn create_trades_table(
    table_root: &Path,
    row_count: usize,
    file_count: usize,
) -> FixtureResult<()> {
    let (metadata_dir, data_dir) = create_table_dirs(table_root)?;
    let rows = generate_trade_rows(row_count);
    let file_count = file_count.max(1);
    let chunk_size = (rows.len() + file_count - 1) / file_count;

    let schema_str = "message trades {
        REQUIRED INT64 timestamp;
        REQUIRED BYTE_ARRAY instrument (UTF8);
        REQUIRED DOUBLE price;
        REQUIRED DOUBLE amount;
        REQUIRED BYTE_ARRAY direction (UTF8);
        OPTIONAL DOUBLE iv;
        REQUIRED DOUBLE index_price;
        REQUIRED BYTE_ARRAY trade_id (UTF8);
    }";
    let schema = Arc::new(parse_message_type(schema_str)?);

    for (file_idx, chunk) in rows.chunks(chunk_size).enumerate() {
        let file_path = data_dir.join(format!("trades_{file_idx:04}.parquet"));
        let file = fs::File::create(&file_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema.clone(), Arc::new(props))?;

        let mut row_group = writer.next_row_group()?;

        // timestamp
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<i64> = chunk.iter().map(|r| r.timestamp).collect();
        col.typed::<Int64Type>().write_batch(&vals, None, None)?;
        col.close()?;

        // instrument
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(r.instrument.as_str())).collect();
        col.typed::<ByteArrayType>().write_batch(&vals, None, None)?;
        col.close()?;

        // price
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<f64> = chunk.iter().map(|r| r.price).collect();
        col.typed::<DoubleType>().write_batch(&vals, None, None)?;
        col.close()?;

        // amount
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<f64> = chunk.iter().map(|r| r.amount).collect();
        col.typed::<DoubleType>().write_batch(&vals, None, None)?;
        col.close()?;

        // direction
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(r.direction.as_str())).collect();
        col.typed::<ByteArrayType>().write_batch(&vals, None, None)?;
        col.close()?;

        // iv (optional)
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<f64> = chunk.iter().map(|r| r.iv.unwrap_or(0.0)).collect();
        let def_levels: Vec<i16> = chunk.iter().map(|r| if r.iv.is_some() { 1 } else { 0 }).collect();
        col.typed::<DoubleType>().write_batch(&vals, Some(&def_levels), None)?;
        col.close()?;

        // index_price
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<f64> = chunk.iter().map(|r| r.index_price).collect();
        col.typed::<DoubleType>().write_batch(&vals, None, None)?;
        col.close()?;

        // trade_id
        let mut col = row_group.next_column()?.unwrap();
        let vals: Vec<ByteArray> = chunk.iter().map(|r| ByteArray::from(r.trade_id.as_str())).collect();
        col.typed::<ByteArrayType>().write_batch(&vals, None, None)?;
        col.close()?;

        row_group.close()?;
        writer.close()?;
    }

    // Write Iceberg metadata
    let metadata = json!({
        "format-version": 2,
        "table-uuid": "test-trades-uuid-001",
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "timestamp", "type": "long", "required": true},
                {"id": 2, "name": "instrument", "type": "string", "required": true},
                {"id": 3, "name": "price", "type": "double", "required": true},
                {"id": 4, "name": "amount", "type": "double", "required": true},
                {"id": 5, "name": "direction", "type": "string", "required": true},
                {"id": 6, "name": "iv", "type": "double", "required": false},
                {"id": 7, "name": "index_price", "type": "double", "required": true},
                {"id": 8, "name": "trade_id", "type": "string", "required": true}
            ]
        }],
        "partition-specs": [],
        "snapshots": []
    });
    fs::write(
        metadata_dir.join("v1.metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;

    Ok(())
}

fn create_ohlcv_table(table_root: &Path, row_count: usize) -> FixtureResult<()> {
    let (metadata_dir, data_dir) = create_table_dirs(table_root)?;

    let schema_str = "message ohlcv {
        REQUIRED INT64 tick;
        REQUIRED DOUBLE open;
        REQUIRED DOUBLE high;
        REQUIRED DOUBLE low;
        REQUIRED DOUBLE close;
        REQUIRED DOUBLE volume;
    }";
    let schema = Arc::new(parse_message_type(schema_str)?);
    let file = fs::File::create(data_dir.join("ohlcv_0000.parquet"))?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;
    let mut rg = writer.next_row_group()?;

    let ticks: Vec<i64> = (0..row_count).map(|i| BASE_TIMESTAMP_MS + i64::try_from(i).unwrap_or(0) * 3_600_000).collect();
    let opens: Vec<f64> = (0..row_count).map(|i| 60_000.0 + f64::from((i * 31 % 3_000) as u32)).collect();
    let highs: Vec<f64> = opens.iter().enumerate().map(|(i, o)| o + f64::from((i * 7 % 500 + 100) as u32)).collect();
    let lows: Vec<f64> = opens.iter().enumerate().map(|(i, o)| o - f64::from((i * 11 % 400 + 50) as u32)).collect();
    let closes: Vec<f64> = opens.iter().enumerate().map(|(i, o)| o + f64::from((i * 3 % 200) as u32) - 100.0).collect();
    let volumes: Vec<f64> = (0..row_count).map(|i| 100.0 + f64::from((i * 53 % 900) as u32)).collect();

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&ticks, None, None)?;
    col.close()?;

    for vals in [&opens, &highs, &lows, &closes, &volumes] {
        let mut col = rg.next_column()?.unwrap();
        col.typed::<DoubleType>().write_batch(vals, None, None)?;
        col.close()?;
    }

    rg.close()?;
    writer.close()?;

    let metadata = json!({
        "format-version": 2,
        "table-uuid": "test-ohlcv-uuid-001",
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "fields": [
            {"id": 1, "name": "tick", "type": "long", "required": true},
            {"id": 2, "name": "open", "type": "double", "required": true},
            {"id": 3, "name": "high", "type": "double", "required": true},
            {"id": 4, "name": "low", "type": "double", "required": true},
            {"id": 5, "name": "close", "type": "double", "required": true},
            {"id": 6, "name": "volume", "type": "double", "required": true}
        ]}],
        "partition-specs": [],
        "snapshots": []
    });
    fs::write(metadata_dir.join("v1.metadata.json"), serde_json::to_string_pretty(&metadata)?)?;
    Ok(())
}

fn create_options_table(table_root: &Path, row_count: usize) -> FixtureResult<()> {
    let (metadata_dir, data_dir) = create_table_dirs(table_root)?;

    let schema_str = "message options {
        REQUIRED INT64 timestamp;
        REQUIRED BYTE_ARRAY instrument (UTF8);
        REQUIRED DOUBLE strike;
        REQUIRED BYTE_ARRAY option_type (UTF8);
        REQUIRED DOUBLE mark_price;
        REQUIRED DOUBLE mark_iv;
        REQUIRED DOUBLE underlying_price;
        REQUIRED DOUBLE delta;
        REQUIRED DOUBLE gamma;
        REQUIRED DOUBLE vega;
        REQUIRED DOUBLE theta;
        REQUIRED DOUBLE open_interest;
    }";
    let schema = Arc::new(parse_message_type(schema_str)?);
    let file = fs::File::create(data_dir.join("options_0000.parquet"))?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;
    let mut rg = writer.next_row_group()?;

    let timestamps: Vec<i64> = (0..row_count).map(|i| BASE_TIMESTAMP_MS + i64::try_from(i).unwrap_or(0) * 300_000).collect();
    let instruments: Vec<ByteArray> = (0..row_count).map(|i| {
        let strike = 50_000 + (i * 1000 % 20_000);
        let opt_type = if i % 2 == 0 { "C" } else { "P" };
        ByteArray::from(format!("BTC-28MAR25-{strike}-{opt_type}").as_str())
    }).collect();
    let strikes: Vec<f64> = (0..row_count).map(|i| 50_000.0 + f64::from((i * 1000 % 20_000) as u32)).collect();
    let option_types: Vec<ByteArray> = (0..row_count).map(|i| ByteArray::from(if i % 2 == 0 { "call" } else { "put" })).collect();
    let mark_prices: Vec<f64> = (0..row_count).map(|i| 0.05 + f64::from((i * 7 % 20) as u32) / 100.0).collect();
    let mark_ivs: Vec<f64> = (0..row_count).map(|i| 0.4 + f64::from((i * 11 % 30) as u32) / 100.0).collect();
    let underlying_prices: Vec<f64> = (0..row_count).map(|i| 60_000.0 + f64::from((i * 41 % 2_000) as u32)).collect();
    let deltas: Vec<f64> = (0..row_count).map(|i| if i % 2 == 0 { 0.3 + f64::from((i % 5) as u32) / 10.0 } else { -0.3 - f64::from((i % 5) as u32) / 10.0 }).collect();
    let gammas: Vec<f64> = (0..row_count).map(|i| 0.00001 + f64::from((i % 10) as u32) / 1_000_000.0).collect();
    let vegas: Vec<f64> = (0..row_count).map(|i| 50.0 + f64::from((i * 13 % 100) as u32)).collect();
    let thetas: Vec<f64> = (0..row_count).map(|i| -(10.0 + f64::from((i * 7 % 50) as u32))).collect();
    let open_interests: Vec<f64> = (0..row_count).map(|i| 100.0 + f64::from((i * 37 % 900) as u32)).collect();

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&timestamps, None, None)?;
    col.close()?;

    let mut col = rg.next_column()?.unwrap();
    col.typed::<ByteArrayType>().write_batch(&instruments, None, None)?;
    col.close()?;

    let mut col = rg.next_column()?.unwrap();
    col.typed::<DoubleType>().write_batch(&strikes, None, None)?;
    col.close()?;

    let mut col = rg.next_column()?.unwrap();
    col.typed::<ByteArrayType>().write_batch(&option_types, None, None)?;
    col.close()?;

    for vals in [&mark_prices, &mark_ivs, &underlying_prices, &deltas, &gammas, &vegas, &thetas, &open_interests] {
        let mut col = rg.next_column()?.unwrap();
        col.typed::<DoubleType>().write_batch(vals, None, None)?;
        col.close()?;
    }

    rg.close()?;
    writer.close()?;

    let metadata = json!({
        "format-version": 2,
        "table-uuid": "test-options-uuid-001",
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "fields": [
            {"id": 1, "name": "timestamp", "type": "long", "required": true},
            {"id": 2, "name": "instrument", "type": "string", "required": true},
            {"id": 3, "name": "strike", "type": "double", "required": true},
            {"id": 4, "name": "option_type", "type": "string", "required": true},
            {"id": 5, "name": "mark_price", "type": "double", "required": true},
            {"id": 6, "name": "mark_iv", "type": "double", "required": true},
            {"id": 7, "name": "underlying_price", "type": "double", "required": true},
            {"id": 8, "name": "delta", "type": "double", "required": true},
            {"id": 9, "name": "gamma", "type": "double", "required": true},
            {"id": 10, "name": "vega", "type": "double", "required": true},
            {"id": 11, "name": "theta", "type": "double", "required": true},
            {"id": 12, "name": "open_interest", "type": "double", "required": true}
        ]}],
        "partition-specs": [],
        "snapshots": []
    });
    fs::write(metadata_dir.join("v1.metadata.json"), serde_json::to_string_pretty(&metadata)?)?;
    Ok(())
}

fn create_multi_schema_table(table_root: &Path) -> FixtureResult<()> {
    let (metadata_dir, data_dir) = create_table_dirs(table_root)?;

    // File 1: schema v1 (2 columns)
    let schema1_str = "message v1 {
        REQUIRED INT64 id;
        REQUIRED INT64 value;
    }";
    let schema1 = Arc::new(parse_message_type(schema1_str)?);
    let file = fs::File::create(data_dir.join("v1_0000.parquet"))?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, schema1, Arc::new(props))?;
    let mut rg = writer.next_row_group()?;

    let ids: Vec<i64> = vec![1, 2, 3, 4, 5];
    let values: Vec<i64> = vec![10, 20, 30, 40, 50];

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&ids, None, None)?;
    col.close()?;

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&values, None, None)?;
    col.close()?;

    rg.close()?;
    writer.close()?;

    // File 2: schema v2 (3 columns — added 'updated_at')
    let schema2_str = "message v2 {
        REQUIRED INT64 id;
        REQUIRED INT64 value;
        REQUIRED INT64 updated_at;
    }";
    let schema2 = Arc::new(parse_message_type(schema2_str)?);
    let file = fs::File::create(data_dir.join("v2_0000.parquet"))?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, schema2, Arc::new(props))?;
    let mut rg = writer.next_row_group()?;

    let ids: Vec<i64> = vec![6, 7, 8];
    let values: Vec<i64> = vec![60, 70, 80];
    let updated_at: Vec<i64> = vec![BASE_TIMESTAMP_MS, BASE_TIMESTAMP_MS + 1000, BASE_TIMESTAMP_MS + 2000];

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&ids, None, None)?;
    col.close()?;

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&values, None, None)?;
    col.close()?;

    let mut col = rg.next_column()?.unwrap();
    col.typed::<Int64Type>().write_batch(&updated_at, None, None)?;
    col.close()?;

    rg.close()?;
    writer.close()?;

    let metadata = json!({
        "format-version": 2,
        "table-uuid": "test-multi-schema-uuid-001",
        "current-schema-id": 1,
        "schemas": [
            {"schema-id": 0, "fields": [
                {"id": 1, "name": "id", "type": "long", "required": true},
                {"id": 2, "name": "value", "type": "long", "required": true}
            ]},
            {"schema-id": 1, "fields": [
                {"id": 1, "name": "id", "type": "long", "required": true},
                {"id": 2, "name": "value", "type": "long", "required": true},
                {"id": 3, "name": "updated_at", "type": "long", "required": true}
            ]}
        ],
        "partition-specs": [],
        "snapshots": []
    });
    fs::write(metadata_dir.join("v1.metadata.json"), serde_json::to_string_pretty(&metadata)?)?;
    Ok(())
}
