use crate::event_bus::{Event, SharedBus};
use serde::Deserialize;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

#[derive(Debug, Deserialize)]
struct TermMessage {
    command: String,
    exit_code: Option<i32>,
    cwd: Option<String>,
    timestamp: Option<u64>,
    phase: Option<String>,
}

/// Starts a Unix socket listener at /tmp/activity_monitor.sock and publishes terminal events to the bus.
pub fn start_terminal_collector(bus: SharedBus) {
    let socket_path = "/tmp/activity_monitor.sock".to_string();
    tokio::spawn(async move {
        // Remove old socket if exists
        if Path::new(&socket_path).exists() {
            let _ = std::fs::remove_file(&socket_path);
        }

        match UnixListener::bind(&socket_path) {
            Ok(listener) => {
                tracing::info!("Terminal collector listening on {}", socket_path);
                loop {
                    match listener.accept().await {
                        Ok((stream, _addr)) => {
                            let bus = bus.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_stream(stream, bus).await {
                                    tracing::error!("terminal handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("accept error: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => tracing::error!("failed to bind terminal socket {}: {}", socket_path, e),
        }
    });
}

async fn handle_stream(stream: UnixStream, bus: SharedBus) -> Result<(), Box<dyn std::error::Error>> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() { continue; }
        match serde_json::from_str::<TermMessage>(&line) {
            Ok(msg) => {
                tracing::info!("Terminal received: {:#?}", msg);
                // Filter out internal hook noise
                let cmd_trim = msg.command.trim();
                if cmd_trim.starts_with("_am_")
                    || cmd_trim.starts_with("PROMPT_COMMAND")
                    || cmd_trim.starts_with("[[")
                    || cmd_trim.is_empty()
                    // Kitty's bash shell-integration script re-sources itself and
                    // fires several internal helper commands on every prompt.
                    || cmd_trim.contains("_ksi_")
                    || cmd_trim.starts_with("builtin ")
                    || cmd_trim.starts_with("case :$SHELLOPTS")
                    || cmd_trim.starts_with("[ \"${BASH_VERSINFO")
                {
                    tracing::debug!("Ignoring internal command: {}", cmd_trim);
                    continue;
                }

                let ts = msg.timestamp.unwrap_or_else(|| { chrono::Utc::now().timestamp() as u64 });
                let ev = Event::Terminal {
                    command: msg.command,
                    exit_code: msg.exit_code,
                    cwd: msg.cwd,
                    phase: msg.phase,
                    timestamp: ts,
                };
                bus.publish_event(ev);
            }
            Err(e) => tracing::warn!("invalid terminal message: {} (line: {})", e, line),
        }
    }
    Ok(())
}
