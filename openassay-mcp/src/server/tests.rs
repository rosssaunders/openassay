use std::sync::{Mutex, OnceLock};

use openassay::tcop::engine::snapshot_state;
use serde_json::{Value, json};

use super::*;
use crate::helpers::quote_qualified_identifier;

fn test_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn drop_public_tables() {
    let server = McpServer::new();
    let tables = server
        .list_tables_data()
        .await
        .unwrap_or_else(|_| Value::Array(Vec::new()));
    let Value::Array(entries) = tables else {
        return;
    };

    for entry in entries {
        let Some(table_name) = entry
            .as_object()
            .and_then(|obj| obj.get("name"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let sql = format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            quote_qualified_identifier(table_name)
                .expect("table name from information_schema should be valid")
        );
        let _ = server.execute_sql(&sql).await;
    }
}

fn decode_tool_payload(result: &Value) -> Value {
    let text = result
        .get("content")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
        .expect("tool response should contain text payload");
    serde_json::from_str(text).expect("tool payload should be valid JSON")
}

#[tokio::test(flavor = "current_thread")]
async fn query_tool_executes_sql() {
    let _guard = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    drop_public_tables().await;

    let mut server = McpServer::with_baseline(snapshot_state());
    let result = server
        .tools_call_result(json!({
            "name": "query",
            "arguments": {
                "sql": "SELECT 1 + 1 AS result"
            }
        }))
        .await
        .expect("query tool should return success envelope");

    let payload = decode_tool_payload(&result);
    assert_eq!(payload["columns"], json!(["result"]));
    assert_eq!(payload["rows"], json!([[2]]));
    assert_eq!(payload["rowCount"], json!(1));
}

#[tokio::test(flavor = "current_thread")]
async fn list_describe_similarity_and_reset_tools_work() {
    let _guard = test_mutex()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    drop_public_tables().await;

    let mut server = McpServer::with_baseline(snapshot_state());
    let _ = server
        .execute_sql("CREATE TABLE items (id int8, emb vector(2))")
        .await
        .expect("table should be created");
    let _ = server
        .execute_sql("INSERT INTO items VALUES (1, '[1,1]'), (2, '[5,5]'), (3, '[2,2]')")
        .await
        .expect("rows should be inserted");

    let list_result = server
        .tools_call_result(json!({
            "name": "list_tables",
            "arguments": {}
        }))
        .await
        .expect("list_tables should return success envelope");
    let list_payload = decode_tool_payload(&list_result);
    assert!(
        list_payload
            .as_array()
            .expect("list_tables payload should be an array")
            .iter()
            .any(|entry| entry["name"] == "items" && entry["columnCount"] == 2)
    );

    let describe_result = server
        .tools_call_result(json!({
            "name": "describe_table",
            "arguments": {
                "table_name": "items"
            }
        }))
        .await
        .expect("describe_table should return success envelope");
    let describe_payload = decode_tool_payload(&describe_result);
    assert_eq!(
        describe_payload["columns"],
        json!([
            {"name": "id", "type": "bigint"},
            {"name": "emb", "type": "vector"}
        ])
    );

    let similarity_result = server
        .tools_call_result(json!({
            "name": "similarity_search",
            "arguments": {
                "table": "items",
                "column": "emb",
                "vector": [1.0, 1.0],
                "k": 2
            }
        }))
        .await
        .expect("similarity_search should return success envelope");
    let similarity_payload = decode_tool_payload(&similarity_result);
    assert_eq!(similarity_payload["columns"], json!(["id", "emb"]));
    assert_eq!(similarity_payload["rows"][0][0], json!(1));
    assert_eq!(similarity_payload["rows"].as_array().map(Vec::len), Some(2));

    let reset_result = server
        .tools_call_result(json!({
            "name": "reset",
            "arguments": {}
        }))
        .await
        .expect("reset should return success envelope");
    let reset_payload = decode_tool_payload(&reset_result);
    assert_eq!(reset_payload, json!({ "status": "ok" }));

    let tables_after_reset = server
        .list_tables_data()
        .await
        .expect("list tables should succeed after reset");
    assert_eq!(tables_after_reset, json!([]));
}
