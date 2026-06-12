//! Stage 1: the sliding window buffer.
//!
//! A bounded, time-keyed view of the most recent normalized events,
//! maintained in memory with a TTL-based eviction. Alongside the
//! chronological list it keeps per-source sublists so pattern recognizers
//! can ask "what have the terminal events looked like in the last 5
//! minutes?" in O(k) (events from that source) rather than O(n) (all
//! events).

use serde_json::Value;
use std::collections::HashMap;

/// Sources the recognizers query via `by_source`. Other sources (e.g.
/// `"session"`, `"process"`, `"calendar"`) are still kept in `events` but
/// don't get a dedicated sublist.
const TRACKED_SOURCES: &[&str] = &["terminal", "browser", "window", "filesystem", "clipboard"];

pub struct SlidingWindowBuffer {
    ttl_secs: u64,
    /// All buffered events, oldest first.
    pub events: Vec<Value>,
    /// Per-source sublists, oldest first.
    pub by_source: HashMap<String, Vec<Value>>,
}

impl SlidingWindowBuffer {
    pub fn new(ttl_secs: u64) -> Self {
        let by_source = TRACKED_SOURCES.iter().map(|s| (s.to_string(), Vec::new())).collect();
        Self { ttl_secs, events: Vec::new(), by_source }
    }

    /// Appends a normalized event and evicts anything older than the TTL
    /// relative to `now` (unix seconds).
    pub fn push(&mut self, event: Value, now: u64) {
        let source = event.get("source").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if let Some(list) = self.by_source.get_mut(&source) {
            list.push(event.clone());
        }
        self.events.push(event);
        self.evict(now);
    }

    /// Drops events older than the TTL relative to `now`, without pushing a
    /// new event - used by the periodic tick so idle detection works even
    /// when nothing new has arrived.
    pub fn evict(&mut self, now: u64) {
        let cutoff = now.saturating_sub(self.ttl_secs);
        self.events.retain(|e| event_ts(e) >= cutoff);
        for list in self.by_source.values_mut() {
            list.retain(|e| event_ts(e) >= cutoff);
        }
    }
}

pub fn event_ts(event: &Value) -> u64 {
    event.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0)
}

pub fn event_entities(event: &Value) -> &Value {
    event.get("entities").unwrap_or(&Value::Null)
}
