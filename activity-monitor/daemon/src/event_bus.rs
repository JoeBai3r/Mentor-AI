use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Heartbeat { timestamp: u64 },
    Window {
        event_type: String,
        application: Option<String>,
        title: Option<String>,
        cwd: Option<String>,
        /// For "blur" events, how long the window held focus before this switch.
        duration_secs: Option<u64>,
        timestamp: u64,
    },
    Terminal {
        command: String,
        exit_code: Option<i32>,
        cwd: Option<String>,
        phase: Option<String>,
        timestamp: u64,
    },
    Browser {
        source: String,
        site: String,
        url: Option<String>,
        title: Option<String>,
        h1: Option<String>,
        selection: Option<String>,
        /// Intent bucket inferred from the domain (docs, research, communication, etc).
        category: Option<String>,
        /// Search query extracted from the URL, when on a search engine.
        search_query: Option<String>,
        timestamp: u64,
    },
    /// A clipboard change. Content is classified rather than stored verbatim
    /// for likely-sensitive copies (see `content_type` == "secret").
    Clipboard {
        content_type: String,
        preview: Option<String>,
        length: usize,
        line_count: usize,
        timestamp: u64,
    },
    /// A file created/modified/removed under a watched project directory.
    FileChange {
        path: String,
        kind: String,
        project: Option<String>,
        timestamp: u64,
    },
    /// Snapshot of locally-listening TCP ports, e.g. dev servers / databases.
    ProcessActivity {
        listening_ports: Vec<ListeningPort>,
        timestamp: u64,
    },
    /// Current and next calendar events, when a local calendar is configured.
    Calendar {
        current_event: Option<CalendarEvent>,
        next_event: Option<CalendarEvent>,
        timestamp: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListeningPort {
    pub port: u16,
    pub process: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarEvent {
    pub summary: String,
    pub start: String,
    pub end: Option<String>,
}

#[derive(Clone)]
pub struct EventBus {
    sender: Arc<broadcast::Sender<Value>>,
    /// Separate channel carrying normalized session events (see `crate::normalize`).
    normalized_sender: Arc<broadcast::Sender<Value>>,
    /// Separate channel carrying `ImmediateState` updates (see `crate::immediate`).
    immediate_sender: Arc<broadcast::Sender<Value>>,
    /// Separate channel carrying the long-term `UserProfile`, republished
    /// whenever the distiller persists an update.
    profile_sender: Arc<broadcast::Sender<Value>>,
    /// Separate channel carrying newly-distilled `SessionIntent`s, one per
    /// closed session.
    session_sender: Arc<broadcast::Sender<Value>>,
    /// Separate channel carrying `Recommendation`s produced by
    /// `crate::recommend` whenever the intervention gate opens.
    recommendation_sender: Arc<broadcast::Sender<Value>>,
}

impl EventBus {
    pub fn new(buffer: usize) -> Self {
        let (sender, _recv) = broadcast::channel(buffer);
        let (normalized_sender, _recv2) = broadcast::channel(buffer);
        let (immediate_sender, _recv3) = broadcast::channel(buffer);
        let (profile_sender, _recv4) = broadcast::channel(buffer);
        let (session_sender, _recv5) = broadcast::channel(buffer);
        let (recommendation_sender, _recv6) = broadcast::channel(buffer);
        Self {
            sender: Arc::new(sender),
            normalized_sender: Arc::new(normalized_sender),
            immediate_sender: Arc::new(immediate_sender),
            profile_sender: Arc::new(profile_sender),
            session_sender: Arc::new(session_sender),
            recommendation_sender: Arc::new(recommendation_sender),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.sender.subscribe()
    }

    /// Best-effort publish from a typed Event
    pub fn publish_event(&self, ev: Event) {
        if let Ok(v) = serde_json::to_value(&ev) {
            let _ = self.sender.send(v);
        }
    }

    /// Best-effort publish a raw JSON value
    pub fn publish_raw(&self, v: Value) {
        let _ = self.sender.send(v);
    }

    /// Subscribe to the normalized event stream produced by `crate::normalize`.
    pub fn subscribe_normalized(&self) -> broadcast::Receiver<Value> {
        self.normalized_sender.subscribe()
    }

    /// Publish a normalized session event (best-effort).
    pub fn publish_normalized(&self, v: Value) {
        let _ = self.normalized_sender.send(v);
    }

    /// Subscribe to the `ImmediateState` stream produced by `crate::immediate`.
    pub fn subscribe_immediate(&self) -> broadcast::Receiver<Value> {
        self.immediate_sender.subscribe()
    }

    /// Publish an `ImmediateState` update (best-effort).
    pub fn publish_immediate(&self, v: Value) {
        let _ = self.immediate_sender.send(v);
    }

    /// Subscribe to `UserProfile` updates published by `crate::distiller`
    /// each time a session is folded into the profile.
    pub fn subscribe_profile(&self) -> broadcast::Receiver<Value> {
        self.profile_sender.subscribe()
    }

    /// Publish an updated `UserProfile` (best-effort).
    pub fn publish_profile(&self, v: Value) {
        let _ = self.profile_sender.send(v);
    }

    /// Subscribe to newly-distilled `SessionIntent`s.
    pub fn subscribe_session_intent(&self) -> broadcast::Receiver<Value> {
        self.session_sender.subscribe()
    }

    /// Publish a newly-distilled `SessionIntent` (best-effort).
    pub fn publish_session_intent(&self, v: Value) {
        let _ = self.session_sender.send(v);
    }

    /// Subscribe to newly-generated `Recommendation`s.
    pub fn subscribe_recommendation(&self) -> broadcast::Receiver<Value> {
        self.recommendation_sender.subscribe()
    }

    /// Publish a newly-generated `Recommendation` (best-effort).
    pub fn publish_recommendation(&self, v: Value) {
        let _ = self.recommendation_sender.send(v);
    }
}

pub type SharedBus = Arc<EventBus>;
