mod helpers;
mod rpc;
mod server;

use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::rpc::process_line;
use crate::server::McpServer;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut server = McpServer::new();
    let mut lines = BufReader::new(io::stdin()).lines();
    let mut stdout = io::stdout();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                eprintln!("stdin read error: {error}");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let response = process_line(&mut server, &line).await;
        if let Some(payload) = response {
            let encoded = payload.to_string();
            if let Err(error) = stdout.write_all(encoded.as_bytes()).await {
                eprintln!("stdout write error: {error}");
                break;
            }
            if let Err(error) = stdout.write_all(b"\n").await {
                eprintln!("stdout write error: {error}");
                break;
            }
            if let Err(error) = stdout.flush().await {
                eprintln!("stdout flush error: {error}");
                break;
            }
        }
    }
}
