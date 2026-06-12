mod event_bus;
mod collectors;
mod distiller;
mod immediate;
mod normalize;
mod profile;
mod recommend;
mod store;

use std::convert::Infallible;


use crate::event_bus::{Event, EventBus, SharedBus};
use std::sync::Arc;
use std::time::Duration;
use warp::Filter;
use warp::ws::{Message, WebSocket};
use futures_util::{StreamExt, SinkExt};
use tokio::time::sleep;
use tracing::Level;


use crate::collectors::start_terminal_collector;
use collectors::start_window_collector;
use crate::collectors::browser::publish_browser_message;
use crate::collectors::{start_calendar_collector, start_clipboard_collector, start_filesystem_collector, start_process_collector};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Load ANTHROPIC_API_KEY / ANTHROPIC_MODEL (and any other overrides) from
    // a .env file in the working directory, if present. Real environment
    // variables always take precedence over the file. See .env.example.
    if let Ok(path) = dotenvy::dotenv() {
        tracing::info!("loaded environment overrides from {}", path.display());
    }

    tracing::info!("Activity daemon starting on :3030");

    // Create shared event bus
    let bus = Arc::new(EventBus::new(1024));

    // Start the event store and normalizer first so they subscribe before
    // any collector can publish (some collectors emit their first event
    // synchronously).
    crate::store::start_event_store(bus.clone());

    // Run raw events through the normalization pipeline (temporal alignment,
    // session boundaries, entity extraction, correlation, noise suppression)
    // before they're sent to the frontend.
    crate::normalize::start_normalizer(bus.clone());

    // Real-time signal classification (error loops, repetition, idle,
    // context switches) over a sliding window of normalized events.
    crate::immediate::start_immediate_layer(bus.clone());

    // Distill closed sessions into SessionIntent objects and fold them into
    // the long-term user profile.
    crate::distiller::start_distiller(bus.clone());

    // Generate a recommendation whenever the intervention gate opens.
    crate::recommend::start_recommendation_engine(bus.clone());

    // Start collectors
    start_window_collector(bus.clone());
    start_terminal_collector(bus.clone());
    start_clipboard_collector(bus.clone());
    start_filesystem_collector(bus.clone());
    start_process_collector(bus.clone());
    start_calendar_collector(bus.clone());

    // Spawn a simple heartbeat publisher
    let hb_bus = bus.clone();
    tokio::spawn(async move {
        loop {
            let ts = chrono::Utc::now().timestamp() as u64;
            hb_bus.publish_event(Event::Heartbeat { timestamp: ts });
            sleep(Duration::from_secs(5)).await;
        }
    });

    // WebSocket route at /ws; pass bus clone into handler
    let bus_for_feedback = bus.clone();
    let bus_filter = warp::any().map(move || bus.clone());
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(bus_filter)
        .and_then(handle_ws_upgrade);

    // API route for recent raw events
    let events_route = warp::path!("api" / "events" / "recent")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and_then(|q: std::collections::HashMap<String, String>| async move {
            let limit = q.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(20);
            let evs = crate::store::read_recent_events(limit);
            Ok::<_, Infallible>(warp::reply::json(&evs))
        })
        .boxed();

    // API route for recent normalized session events
    let normalized_route = warp::path!("api" / "events" / "normalized" / "recent")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and_then(|q: std::collections::HashMap<String, String>| async move {
            let limit = q.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(20);
            let evs = crate::store::read_recent_normalized_events(limit);
            Ok::<_, Infallible>(warp::reply::json(&evs))
        })
        .boxed();

    // Long-term user profile (Group 1-7 schema in `crate::profile`).
    let profile_get_route = warp::path!("api" / "profile")
        .and(warp::get())
        .map(|| warp::reply::json(&crate::store::load_profile()))
        .boxed();

    let profile_put_route = warp::path!("api" / "profile")
        .and(warp::put())
        .and(warp::body::json())
        .map(|profile: crate::profile::UserProfile| {
            match crate::store::save_profile(&profile) {
                Ok(()) => warp::reply::with_status(warp::reply::json(&profile), warp::http::StatusCode::OK),
                Err(e) => {
                    tracing::error!("failed to save profile: {}", e);
                    warp::reply::with_status(warp::reply::json(&serde_json::json!({"error": e.to_string()})), warp::http::StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        })
        .boxed();

    // API route for recently distilled session intents
    let sessions_route = warp::path!("api" / "sessions" / "recent")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and_then(|q: std::collections::HashMap<String, String>| async move {
            let limit = q.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(20);
            let sessions = crate::store::read_recent_session_intents(limit);
            Ok::<_, Infallible>(warp::reply::json(&sessions))
        })
        .boxed();

    // API route for recent recommendations
    let recommendations_route = warp::path!("api" / "recommendations" / "recent")
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and_then(|q: std::collections::HashMap<String, String>| async move {
            let limit = q.get("limit").and_then(|s| s.parse::<usize>().ok()).unwrap_or(20);
            let recs = crate::store::read_recent_recommendations(limit);
            Ok::<_, Infallible>(warp::reply::json(&recs))
        })
        .boxed();

    // API route for accept/dismiss feedback on a recommendation. Also
    // republishes the updated profile so the dashboard's profile panel
    // reflects the new accept rate immediately.
    let recommendation_feedback_route = warp::path!("api" / "recommendations" / i64 / "feedback")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || bus_for_feedback.clone()))
        .map(|id: i64, body: serde_json::Value, bus: SharedBus| {
            let accepted = body.get("accepted").and_then(|v| v.as_bool()).unwrap_or(false);
            match crate::recommend::apply_feedback(id, accepted) {
                Some((rec, profile)) => {
                    if let Ok(v) = serde_json::to_value(&profile) {
                        bus.publish_profile(v);
                    }
                    warp::reply::with_status(warp::reply::json(&rec), warp::http::StatusCode::OK)
                }
                None => warp::reply::with_status(
                    warp::reply::json(&serde_json::json!({"error": "recommendation not found"})),
                    warp::http::StatusCode::NOT_FOUND,
                ),
            }
        })
        .boxed();

    let routes = ws_route.boxed()
        .or(events_route)
        .or(normalized_route)
        .or(profile_get_route)
        .or(profile_put_route)
        .or(sessions_route)
        .or(recommendations_route)
        .or(recommendation_feedback_route)
        .with(warp::log("activity_daemon"));

    // Allow CORS from local frontend during development
    let cors = warp::cors().allow_any_origin();

    warp::serve(routes.with(cors)).run(([127,0,0,1], 3030)).await;
}

async fn handle_ws_upgrade(ws: warp::ws::Ws, bus: SharedBus) -> Result<impl warp::Reply, Infallible> {
    Ok(ws.on_upgrade(move |socket| client_connection(socket, bus)))
}

async fn client_connection(ws: WebSocket, bus: SharedBus) {
    // Split into sink (tx) and stream (rx)
    let (tx, mut rx) = ws.split();

    // Subscribe to the normalized event stream (post temporal-alignment,
    // sessionization, entity extraction, correlation, noise suppression) -
    // raw collector events are no longer sent to the frontend directly -
    // plus the real-time immediate-intent, session-intent, and long-term
    // profile streams. Each is forwarded as `{"type": ..., "data": ...}` so
    // the frontend can route them to the right panel.
    let mut normalized_sub = bus.subscribe_normalized();
    let mut immediate_sub = bus.subscribe_immediate();
    let mut session_sub = bus.subscribe_session_intent();
    let mut profile_sub = bus.subscribe_profile();
    let mut recommendation_sub = bus.subscribe_recommendation();

    // Task: forward events from EventBus to websocket (owns tx)
    let forward_task = {
        let mut tx = tx;
        tokio::spawn(async move {
            // Send the current profile immediately on connect so the
            // frontend has something to render before the next session
            // closes.
            let initial_profile = serde_json::json!({"type": "profile", "data": crate::store::load_profile()});
            if tx.send(Message::text(initial_profile.to_string())).await.is_err() {
                return;
            }

            loop {
                let envelope = tokio::select! {
                    ev = normalized_sub.recv() => match ev {
                        Ok(v) => Some(serde_json::json!({"type": "normalized", "data": v})),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("client lagging, skipped {} normalized messages", n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                    ev = immediate_sub.recv() => match ev {
                        Ok(v) => Some(serde_json::json!({"type": "immediate", "data": v})),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("client lagging, skipped {} immediate messages", n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => continue,
                    },
                    ev = session_sub.recv() => match ev {
                        Ok(v) => Some(serde_json::json!({"type": "session", "data": v})),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("client lagging, skipped {} session messages", n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => continue,
                    },
                    ev = profile_sub.recv() => match ev {
                        Ok(v) => Some(serde_json::json!({"type": "profile", "data": v})),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("client lagging, skipped {} profile messages", n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => continue,
                    },
                    ev = recommendation_sub.recv() => match ev {
                        Ok(v) => Some(serde_json::json!({"type": "recommendation", "data": v})),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("client lagging, skipped {} recommendation messages", n);
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => continue,
                    },
                };

                if let Some(envelope) = envelope {
                    if tx.send(Message::text(envelope.to_string())).await.is_err() {
                        break;
                    }
                }
            }
        })
    };

    // Task: read incoming messages from client (for future commands)
    while let Some(result) = rx.next().await {
        match result {
            Ok(msg) => {
                if msg.is_text() {
                    let txt = msg.to_str().unwrap_or("");
                    tracing::info!("Received from client: {}", txt);
                    // Try to treat incoming message as browser payload and publish
                    publish_browser_message(bus.clone(), txt);
                }
            }
            Err(e) => {
                tracing::error!("ws error: {}", e);
                break;
            }
        }
    }

    // If client disconnected, ensure forward task ends
    forward_task.abort();
    tracing::info!("WebSocket client disconnected");
}
