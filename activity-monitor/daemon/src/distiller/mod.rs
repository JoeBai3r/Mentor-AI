//! The session-level intent layer ("the distiller").
//!
//! Subscribes to the normalized event stream and buffers events for the
//! current `session_id`. When a `session_boundary` event arrives (the
//! normalizer has just incremented `session_id`), the buffered events
//! describe a session that just ended:
//!
//! 1. **Session summarizer** (this module): compresses the buffered events
//!    into a [`SessionIntent`] - a structured summary of what happened
//!    (projects touched, commands run, friction encountered, inferred work
//!    phases, ...). Persisted to the `session_intents` table.
//! 2. **Aggregation + decay/promotion** ([`aggregate`]): folds the
//!    `SessionIntent` into the long-term [`crate::profile::UserProfile`]
//!    using the strategy documented on each profile field (recency-weighted
//!    frequency, rolling average, or pattern accumulation), then persists
//!    the updated profile.
//!
//! All of this is plain arithmetic - no model calls. A model only enters
//! later, for higher-level judgment calls (naming/explaining a friction
//! pattern, classifying a session's overall goal) that this layer's output
//! feeds.

mod aggregate;
pub(crate) use aggregate::{ema, EMA_ALPHA};

use crate::event_bus::SharedBus;
use chrono::{TimeZone, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Tools whose mere invocation (regardless of subcommand) indicates a test
/// run.
const TEST_TOOLS: &[&str] = &["pytest", "jest", "vitest", "mocha", "rspec"];

/// How many subsequent events to look at when classifying how a test
/// failure was resolved.
const RESOLUTION_LOOKAHEAD: usize = 15;

/// A structured summary of one session, built by compressing its normalized
/// event stream. This is the "session intent object" - the middle of the
/// three intent levels (long-term profile, session, immediate).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionIntent {
    pub session_id: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub duration_min: f64,
    /// Why this session ended: `"inactivity_gap"`, `"project_switch"`,
    /// `"git_commit"`, or `"unclassified"` if the daemon stopped mid-session.
    pub end_reason: String,

    pub event_count: u64,

    /// Project name (not full path - see [`aggregate`] for why) -> number
    /// of normalized events that referenced it.
    pub project_event_counts: HashMap<String, u64>,
    /// Project name -> branches seen during the session, most recent last.
    pub project_branches: HashMap<String, Vec<String>>,
    /// The project with the most events this session, if any.
    pub primary_project: Option<String>,

    /// Canonical app name -> number of focus/launch events.
    pub app_focus_counts: HashMap<String, u64>,
    /// Distinct local hours-of-day (0-23) touched by any event.
    pub active_hours: Vec<u8>,

    pub commands_run: u64,
    pub commands_failed: u64,
    /// Tool name -> number of commands run with that tool.
    pub tool_counts: HashMap<String, u64>,
    /// Ordered `"tool subcommand"` (or just `"tool"`) tokens, for n-gram
    /// extraction in [`aggregate`].
    pub command_sequence: Vec<String>,

    pub files_changed: u64,

    /// Domain -> number of navigations.
    pub browser_domain_counts: HashMap<String, u64>,
    /// Domain -> number of navigations categorized `"documentation"` or
    /// `"error_lookup"` to that domain.
    pub doc_domain_counts: HashMap<String, u64>,
    /// Browser category (from the extension's `CATEGORY_RULES`) -> count.
    pub browser_category_counts: HashMap<String, u64>,
    pub search_queries: Vec<String>,

    pub clipboard_events: u64,

    /// Failed test-command runs, and which tool ran them.
    pub test_failures: u64,
    pub test_failure_tools: HashMap<String, u64>,
    /// One entry per test failure: `"doc_lookup"`, `"trial_error"`, or
    /// `"step_away"`, based on what happened next in the session.
    pub friction_resolutions: Vec<String>,

    /// `Some(true)` if the first "research" navigation (search query, or a
    /// documentation/error-lookup page) came before the first failed
    /// command, `Some(false)` if a failed command came first, `None` if the
    /// session doesn't contain both signals.
    pub search_before_try: Option<bool>,

    /// Inferred work phase (`"implementation"`, `"debugging"`,
    /// `"research"`, `"exploration"`) -> number of events classified into
    /// it.
    pub phase_counts: HashMap<String, u64>,
    /// The phase with the most events this session.
    pub dominant_phase: String,

    // --- Enrichment from the immediate intent layer (`crate::immediate`) ---
    /// `ImmediateState.signal` -> number of times it was the dominant
    /// signal in an emitted state this session. `"none"` is excluded.
    pub immediate_signal_counts: HashMap<String, u64>,
    /// One entry per distinct tool that triggered an `error_loop` signal,
    /// keeping the longest-running episode observed for that tool.
    pub error_loops: Vec<ErrorLoopSummary>,
    /// Highest `blocking_score` observed across the session.
    pub max_blocking_score: f64,
    /// `blocking_score` of the last emitted `ImmediateState`.
    pub final_blocking_score: f64,
    /// `final_blocking_score` minus the first observed `blocking_score`.
    /// Positive means things looked worse by session end (likely
    /// unresolved); negative or zero means the situation eased.
    pub blocking_score_trend: f64,
    /// Number of emitted `ImmediateState`s whose gate was `"open:*"` -
    /// how many moments this session were flagged as good intervention
    /// opportunities.
    pub gate_open_count: u64,
}

/// A distinct error-loop episode (failure -> search -> retry of the same
/// tool) detected by the immediate intent layer during a session.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorLoopSummary {
    pub tool: String,
    pub subcommand: Option<String>,
    pub failure_count: u64,
    pub search_count: u64,
    pub duration_sec: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}

/// Subscribes to the normalized event stream and the immediate intent
/// layer's stream, builds a [`SessionIntent`] each time a session closes
/// (enriched with the immediate layer's signals), and folds it into the
/// long-term profile.
pub fn start_distiller(bus: SharedBus) {
    // Subscribe synchronously so no normalized event / immediate state
    // published by earlier-started layers can be missed.
    let mut rx = bus.subscribe_normalized();
    let mut imm_rx = bus.subscribe_immediate();
    tokio::spawn(async move {
        let mut buffer: Vec<Value> = Vec::new();
        let mut immediate_buffer: Vec<Value> = Vec::new();
        let mut current_session_id: Option<u64> = None;
        let mut profile = crate::store::load_profile();

        loop {
            tokio::select! {
                ev = rx.recv() => {
                    match ev {
                        Ok(val) => {
                            let kind = val.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                            let session_id = val.get("session_id").and_then(|v| v.as_u64());

                            if kind == "session_boundary" {
                                if let Some(closed_id) = current_session_id {
                                    if !buffer.is_empty() {
                                        let end_ts = val.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
                                        let end_reason = val
                                            .get("entities")
                                            .and_then(|e| e.get("category"))
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("unclassified")
                                            .to_string();

                                        let session_immediate: Vec<&Value> = immediate_buffer
                                            .iter()
                                            .filter(|s| s.get("session_id").and_then(|v| v.as_u64()) == Some(closed_id))
                                            .collect();

                                        let session = summarize_session(closed_id, &buffer, end_ts, end_reason, &session_immediate);
                                        let recent = crate::store::read_recent_session_intents(19);
                                        aggregate::distill(&mut profile, &session, &recent);

                                        if let Err(e) = crate::store::store_session_intent(&session) {
                                            tracing::error!("failed to store session intent: {}", e);
                                        }
                                        if let Err(e) = crate::store::save_profile(&profile) {
                                            tracing::error!("failed to save profile: {}", e);
                                        }

                                        if let Ok(v) = serde_json::to_value(&session) {
                                            bus.publish_session_intent(v);
                                        }
                                        if let Ok(v) = serde_json::to_value(&profile) {
                                            bus.publish_profile(v);
                                        }
                                    }
                                    buffer.clear();
                                    immediate_buffer.clear();
                                }
                                current_session_id = session_id;
                                continue;
                            }

                            if current_session_id.is_none() {
                                current_session_id = session_id;
                            }
                            buffer.push(val);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("distiller lagged: {} normalized events dropped", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                imm = imm_rx.recv() => {
                    match imm {
                        Ok(val) => immediate_buffer.push(val),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("distiller lagged: {} immediate states dropped", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
                    }
                }
            }
        }
    });
}

/// Compresses one session's worth of normalized events - enriched with the
/// `ImmediateState`s the immediate intent layer emitted during it - into a
/// [`SessionIntent`].
fn summarize_session(session_id: u64, events: &[Value], end_ts: u64, end_reason: String, immediate: &[&Value]) -> SessionIntent {
    let mut session = SessionIntent {
        session_id,
        end_ts,
        end_reason,
        ..Default::default()
    };

    let mut start_ts = u64::MAX;
    let mut last_ts = 0u64;
    let mut first_failed_command_idx: Option<usize> = None;
    let mut first_research_nav_idx: Option<usize> = None;

    for (idx, ev) in events.iter().enumerate() {
        session.event_count += 1;

        let timestamp = ev.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
        if timestamp > 0 {
            start_ts = start_ts.min(timestamp);
            last_ts = last_ts.max(timestamp);
            if let Some(dt) = chrono::Local.timestamp_opt(timestamp as i64, 0).single() {
                let hour = dt.hour() as u8;
                if !session.active_hours.contains(&hour) {
                    session.active_hours.push(hour);
                }
            }
        }

        let source = ev.get("source").and_then(|v| v.as_str()).unwrap_or("");
        let kind = ev.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let entities = ev.get("entities").cloned().unwrap_or(Value::Null);
        let correlated: Vec<String> = ev
            .get("correlated_with")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();

        if let Some(project) = entities.get("project").and_then(|v| v.as_str()) {
            *session.project_event_counts.entry(project.to_string()).or_insert(0) += 1;
            if let Some(branch) = entities.get("branch").and_then(|v| v.as_str()) {
                let branches = session.project_branches.entry(project.to_string()).or_default();
                if branches.last().map(String::as_str) != Some(branch) {
                    branches.push(branch.to_string());
                }
            }
        }

        match source {
            "window" => {
                if kind == "focus" || kind == "launch" {
                    if let Some(app) = entities.get("app").and_then(|v| v.as_str()) {
                        *session.app_focus_counts.entry(app.to_string()).or_insert(0) += 1;
                    }
                }
            }
            "terminal" => {
                session.commands_run += 1;
                let tool = entities.get("tool").and_then(|v| v.as_str());
                let subcommand = entities.get("subcommand").and_then(|v| v.as_str());
                let exit_code = entities.get("exit_code").and_then(|v| v.as_i64());

                if let Some(tool) = tool {
                    *session.tool_counts.entry(tool.to_string()).or_insert(0) += 1;
                    let token = match subcommand {
                        Some(sub) => format!("{} {}", tool, sub),
                        None => tool.to_string(),
                    };
                    session.command_sequence.push(token);
                }

                let failed = matches!(exit_code, Some(code) if code != 0);
                if failed {
                    session.commands_failed += 1;
                    if first_failed_command_idx.is_none() {
                        first_failed_command_idx = Some(idx);
                    }
                }

                let is_test = subcommand == Some("test") || tool.map(|t| TEST_TOOLS.contains(&t)).unwrap_or(false);
                if is_test && failed {
                    session.test_failures += 1;
                    if let Some(tool) = tool {
                        *session.test_failure_tools.entry(tool.to_string()).or_insert(0) += 1;
                    }
                    session.friction_resolutions.push(classify_resolution(events, idx));
                }
            }
            "filesystem" => {
                session.files_changed += 1;
            }
            "browser" => {
                let category = entities.get("category").and_then(|v| v.as_str());
                if let Some(domain) = entities.get("domain").and_then(|v| v.as_str()) {
                    *session.browser_domain_counts.entry(domain.to_string()).or_insert(0) += 1;
                    if matches!(category, Some("documentation") | Some("error_lookup")) {
                        *session.doc_domain_counts.entry(domain.to_string()).or_insert(0) += 1;
                    }
                }
                if let Some(category) = category {
                    *session.browser_category_counts.entry(category.to_string()).or_insert(0) += 1;
                }
                let has_search = entities.get("search_query").and_then(|v| v.as_str()).map(|q| {
                    session.search_queries.push(q.to_string());
                    true
                }).unwrap_or(false);
                let is_research = has_search || matches!(category, Some("documentation") | Some("error_lookup"));
                if is_research && first_research_nav_idx.is_none() {
                    first_research_nav_idx = Some(idx);
                }
            }
            "clipboard" => {
                session.clipboard_events += 1;
            }
            _ => {}
        }

        if let Some(phase) = classify_phase(kind, &entities, &correlated) {
            *session.phase_counts.entry(phase.to_string()).or_insert(0) += 1;
        }
    }

    if start_ts == u64::MAX {
        start_ts = end_ts;
    }
    session.start_ts = start_ts;
    if session.end_ts == 0 {
        session.end_ts = last_ts.max(start_ts);
    }
    session.duration_min = session.end_ts.saturating_sub(session.start_ts) as f64 / 60.0;

    session.primary_project = session
        .project_event_counts
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(name, _)| name.clone());

    session.dominant_phase = session
        .phase_counts
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(phase, _)| phase.clone())
        .unwrap_or_else(|| "implementation".to_string());

    session.search_before_try = match (first_research_nav_idx, first_failed_command_idx) {
        (Some(r), Some(f)) => Some(r < f),
        _ => None,
    };

    summarize_immediate_states(&mut session, immediate);

    session
}

/// Folds the `ImmediateState`s emitted during this session into the
/// `SessionIntent`'s enrichment fields: signal counts, distinct error-loop
/// episodes (keeping the longest-running per tool), the blocking-score
/// trend (first -> last observed score), and how many emissions had an
/// open intervention gate.
fn summarize_immediate_states(session: &mut SessionIntent, immediate: &[&Value]) {
    let mut first_score: Option<f64> = None;

    for state in immediate {
        let signal = state.get("signal").and_then(|v| v.as_str()).unwrap_or("none");
        if signal != "none" {
            *session.immediate_signal_counts.entry(signal.to_string()).or_insert(0) += 1;
        }

        if signal == "error_loop" {
            if let Some(ctx) = state.get("error_context") {
                let tool = ctx.get("tool").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let duration_sec = ctx.get("duration_sec").and_then(|v| v.as_u64()).unwrap_or(0);
                let failure_count = ctx.get("failure_count").and_then(|v| v.as_u64()).unwrap_or(0);
                let search_count = ctx.get("search_count").and_then(|v| v.as_u64()).unwrap_or(0);
                let subcommand = ctx.get("subcommand").and_then(|v| v.as_str()).map(String::from);
                let domain = ctx.get("domain").and_then(|v| v.as_str()).map(String::from);

                match session.error_loops.iter_mut().find(|e| e.tool == tool) {
                    Some(existing) if duration_sec > existing.duration_sec => {
                        existing.subcommand = subcommand;
                        existing.failure_count = failure_count;
                        existing.search_count = search_count;
                        existing.duration_sec = duration_sec;
                        existing.domain = domain;
                    }
                    Some(_) => {}
                    None => session.error_loops.push(ErrorLoopSummary { tool, subcommand, failure_count, search_count, duration_sec, domain }),
                }
            }
        }

        let score = state.get("blocking_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        if first_score.is_none() {
            first_score = Some(score);
        }
        session.max_blocking_score = session.max_blocking_score.max(score);
        session.final_blocking_score = score;

        let gate = state.get("gate").and_then(|v| v.as_str()).unwrap_or("");
        if gate.starts_with("open") {
            session.gate_open_count += 1;
        }
    }

    session.blocking_score_trend = session.final_blocking_score - first_score.unwrap_or(0.0);
}

/// Classifies a single normalized event into a coarse work phase. Returns
/// `None` for events that don't carry phase signal on their own (window
/// blur/focus churn, passive process/calendar snapshots, clipboard).
fn classify_phase(kind: &str, entities: &Value, correlated: &[String]) -> Option<&'static str> {
    match kind {
        "command" => {
            let exit_code = entities.get("exit_code").and_then(|v| v.as_i64());
            match exit_code {
                Some(code) if code != 0 => Some("debugging"),
                _ => Some("implementation"),
            }
        }
        "file_change" => Some("implementation"),
        "navigation" => {
            let category = entities.get("category").and_then(|v| v.as_str());
            let has_search = entities.get("search_query").and_then(|v| v.as_str()).is_some();
            if correlated.iter().any(|c| c == "recent_test_failure") || category == Some("error_lookup") {
                Some("debugging")
            } else if has_search || category == Some("documentation") {
                Some("research")
            } else {
                Some("exploration")
            }
        }
        _ => None,
    }
}

/// Looks at the events following a test failure (up to
/// [`RESOLUTION_LOOKAHEAD`] of them, or the end of the session) to guess how
/// the user responded: looked up docs/errors, tried something else in the
/// terminal, or the session ended without further action.
fn classify_resolution(events: &[Value], failure_idx: usize) -> String {
    let end = (failure_idx + 1 + RESOLUTION_LOOKAHEAD).min(events.len());
    for ev in &events[failure_idx + 1..end] {
        let source = ev.get("source").and_then(|v| v.as_str()).unwrap_or("");
        let kind = ev.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        if source == "browser" && kind == "navigation" {
            let category = ev.get("entities").and_then(|e| e.get("category")).and_then(|v| v.as_str());
            if matches!(category, Some("documentation") | Some("error_lookup")) {
                return "doc_lookup".to_string();
            }
        }
        if source == "terminal" && kind == "command" {
            return "trial_error".to_string();
        }
    }
    "step_away".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn summarize_session_compresses_normalized_events() {
        let events = vec![
            json!({
                "seq": 1, "timestamp": 1_000, "session_id": 1,
                "source": "window", "kind": "focus",
                "summary": "focused vscode",
                "entities": {"app": "vscode", "project": "activity-monitor"}
            }),
            json!({
                "seq": 2, "timestamp": 1_010, "session_id": 1,
                "source": "terminal", "kind": "command",
                "summary": "cargo test",
                "entities": {"tool": "cargo", "subcommand": "test", "exit_code": 1, "project": "activity-monitor", "branch": "main"}
            }),
            json!({
                "seq": 3, "timestamp": 1_020, "session_id": 1,
                "source": "browser", "kind": "navigation",
                "summary": "docs.rs",
                "entities": {"domain": "doc.rust-lang.org", "category": "documentation"}
            }),
        ];

        let session = summarize_session(1, &events, 1_030, "inactivity_gap".to_string(), &[]);

        assert_eq!(session.session_id, 1);
        assert_eq!(session.start_ts, 1_000);
        assert_eq!(session.end_ts, 1_030);
        assert_eq!(session.event_count, 3);
        assert_eq!(session.primary_project, Some("activity-monitor".to_string()));
        assert_eq!(session.project_branches.get("activity-monitor"), Some(&vec!["main".to_string()]));
        assert_eq!(session.app_focus_counts.get("vscode"), Some(&1));
        assert_eq!(session.commands_run, 1);
        assert_eq!(session.commands_failed, 1);
        assert_eq!(session.test_failures, 1);
        assert_eq!(session.test_failure_tools.get("cargo"), Some(&1));
        assert_eq!(session.friction_resolutions, vec!["doc_lookup".to_string()]);
        assert_eq!(session.doc_domain_counts.get("doc.rust-lang.org"), Some(&1));
        // The doc-lookup navigation came after the failed command in this
        // sequence, so search_before_try is Some(false), not None.
        assert_eq!(session.search_before_try, Some(false));
    }

    #[test]
    fn summarize_session_folds_in_immediate_states() {
        let events = vec![json!({
            "seq": 1, "timestamp": 1_000, "session_id": 1,
            "source": "terminal", "kind": "command",
            "summary": "pytest",
            "entities": {"tool": "pytest", "subcommand": "test", "exit_code": 1, "project": "activity-monitor"}
        })];

        let immediate_owned = vec![
            json!({
                "ts": 1_000, "session_id": 1, "signal": "none",
                "blocking_score": 0.1, "blocking_score_delta": 0.0, "gate": "closed:no_trigger"
            }),
            json!({
                "ts": 1_100, "session_id": 1, "signal": "error_loop",
                "error_context": {"tool": "pytest", "subcommand": "test", "failure_count": 2, "search_count": 1, "duration_sec": 150, "domain": "stackoverflow.com"},
                "blocking_score": 0.6, "blocking_score_delta": 0.5, "gate": "open:extended_block"
            }),
            // A different session's state should be ignored entirely.
            json!({
                "ts": 1_200, "session_id": 2, "signal": "error_loop",
                "error_context": {"tool": "cargo", "subcommand": "build", "failure_count": 5, "search_count": 5, "duration_sec": 9_999, "domain": "example.com"},
                "blocking_score": 0.99, "blocking_score_delta": 0.0, "gate": "open:extended_block"
            }),
        ];
        let immediate: Vec<&Value> = immediate_owned.iter().filter(|s| s.get("session_id").and_then(|v| v.as_u64()) == Some(1)).collect();

        let session = summarize_session(1, &events, 1_200, "inactivity_gap".to_string(), &immediate);

        assert_eq!(session.immediate_signal_counts.get("error_loop"), Some(&1));
        assert_eq!(session.immediate_signal_counts.get("none"), None);
        assert_eq!(session.error_loops.len(), 1);
        assert_eq!(session.error_loops[0].tool, "pytest");
        assert_eq!(session.error_loops[0].duration_sec, 150);
        assert_eq!(session.error_loops[0].domain.as_deref(), Some("stackoverflow.com"));
        assert_eq!(session.max_blocking_score, 0.6);
        assert_eq!(session.final_blocking_score, 0.6);
        assert!((session.blocking_score_trend - 0.5).abs() < 1e-9);
        assert_eq!(session.gate_open_count, 1);
    }
}
