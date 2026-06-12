use crate::event_bus::{Event, SharedBus};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BrowserPayload {
    source: Option<String>,
    site: Option<String>,
    url: Option<String>,
    title: Option<String>,
    h1: Option<String>,
    selection: Option<String>,
    category: Option<String>,
    search_query: Option<String>,
    timestamp: Option<u64>,
}

/// Try to parse a JSON string produced by the browser extension and publish it to the bus.
pub fn publish_browser_message(bus: SharedBus, txt: &str) {
    match serde_json::from_str::<BrowserPayload>(txt) {
        Ok(p) => {
            let ts = p.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp() as u64);
            let ev = Event::Browser {
                source: p.source.unwrap_or_else(|| "browser".to_string()),
                site: p.site.unwrap_or_else(|| p.url.clone().unwrap_or_default()),
                url: p.url,
                title: p.title,
                h1: p.h1,
                selection: p.selection,
                category: p.category,
                search_query: p.search_query,
                timestamp: ts,
            };
            bus.publish_event(ev);
        }
        Err(e) => tracing::warn!("failed to parse browser payload: {} -> {}", e, txt),
    }
}
