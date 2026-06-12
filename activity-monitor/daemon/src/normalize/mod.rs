mod entities;
mod session;

use crate::event_bus::SharedBus;
use entities::{extract_domain, is_git_commit, is_test_command, parse_command, parse_window_title, WindowEntities};
use serde::Serialize;
use serde_json::Value;
use session::{is_background_app, NormalizerState, FLICKER_THRESHOLD_SECS};
use std::path::Path;

/// Structured entities pulled out of a raw event by Stage 3 (entity
/// extraction) and Stage 4 (cross-source correlation). Fields that don't
/// apply to a given event are omitted from the JSON output.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Entities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subcommand: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

impl Entities {
    fn is_empty(&self) -> bool {
        self.app.is_none()
            && self.project.is_none()
            && self.branch.is_none()
            && self.file.is_none()
            && self.domain.is_none()
            && self.category.is_none()
            && self.search_query.is_none()
            && self.tool.is_none()
            && self.subcommand.is_none()
            && self.exit_code.is_none()
            && self.duration_secs.is_none()
            && self.content_type.is_none()
    }
}

/// A single event on the normalized session timeline, ready for inference:
/// monotonically sequenced, structured entities, session-scoped, and
/// annotated with cross-source correlations.
#[derive(Debug, Clone, Serialize)]
pub struct NormalizedEvent {
    /// Monotonic sequence number — use this for ordering, not `timestamp`.
    pub seq: u64,
    pub timestamp: u64,
    pub session_id: u64,
    pub source: String,
    pub kind: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Entities::is_empty")]
    pub entities: Entities,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub correlated_with: Vec<String>,
}

/// Subscribes to the raw event bus, runs every event through the
/// normalization pipeline, and republishes the results on the bus's
/// normalized channel for the frontend.
pub fn start_normalizer(bus: SharedBus) {
    // Subscribe synchronously (before spawning) so no events published by
    // collectors started after this can be missed.
    let mut rx = bus.subscribe();
    tokio::spawn(async move {
        let mut state = NormalizerState::new();
        loop {
            match rx.recv().await {
                Ok(val) => {
                    for ev in process_event(&mut state, &val) {
                        if let Ok(v) = serde_json::to_value(&ev) {
                            bus.publish_normalized(v);
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("normalizer lagged: {} raw events dropped", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

/// Runs a single raw bus event (`{"VariantName": {...}}`) through Stages
/// 2-5, returning zero or more normalized events (a session boundary
/// followed by the event itself, for example).
fn process_event(state: &mut NormalizerState, val: &Value) -> Vec<NormalizedEvent> {
    let mut out = Vec::new();

    let obj = match val.as_object() {
        Some(o) if o.len() == 1 => o,
        _ => return out,
    };
    let (variant, inner) = obj.iter().next().unwrap();
    let inner = match inner.as_object() {
        Some(o) => o,
        None => return out,
    };

    let timestamp = inner.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);

    match variant.as_str() {
        // Synthetic liveness ping - no signal about user activity.
        "Heartbeat" => {}

        "Window" => {
            let event_type = inner.get("event_type").and_then(|v| v.as_str()).unwrap_or("");
            let application = inner.get("application").and_then(|v| v.as_str()).map(String::from);
            let title = inner.get("title").and_then(|v| v.as_str()).map(String::from);
            let cwd = inner.get("cwd").and_then(|v| v.as_str()).map(String::from);
            let duration_secs = inner.get("duration_secs").and_then(|v| v.as_u64());

            if let Some(boundary) = state.note_activity(timestamp) {
                out.push(boundary);
            }

            // Background apps still count as "activity" for gap detection
            // (handled above) but aren't surfaced as their own events, and
            // don't clear an off-task browsing run (the user may have just
            // switched away from the off-task browser tab to a background
            // notifier and back).
            if is_background_app(&application) {
                return out;
            }
            state.note_on_task_activity();

            // Alt-tab-and-back flicker: a blur with near-zero dwell time.
            if event_type == "blur" && duration_secs.unwrap_or(u64::MAX) < FLICKER_THRESHOLD_SECS {
                return out;
            }

            let parsed = parse_window_title(&application, &title, &cwd);

            if let Some(project) = &parsed.project {
                if let Some(boundary) = state.note_project(project, timestamp) {
                    out.push(boundary);
                }
            }

            let summary = match event_type {
                "focus" => format!("Focused {}", describe_window(&parsed, &title)),
                "blur" => format!("Left {} after {}s", describe_window(&parsed, &title), duration_secs.unwrap_or(0)),
                "launch" => format!("Launched {}", describe_window(&parsed, &title)),
                other => format!("{} {}", other, describe_window(&parsed, &title)),
            };

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "window".to_string(),
                kind: event_type.to_string(),
                summary,
                entities: Entities {
                    app: parsed.app,
                    project: parsed.project,
                    branch: parsed.branch,
                    file: parsed.file,
                    duration_secs,
                    ..Default::default()
                },
                correlated_with: vec![],
            });
        }

        "Terminal" => {
            let command = inner.get("command").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let exit_code = inner.get("exit_code").and_then(|v| v.as_i64()).map(|v| v as i32);
            let cwd = inner.get("cwd").and_then(|v| v.as_str()).map(String::from);
            let phase = inner.get("phase").and_then(|v| v.as_str());

            if let Some(boundary) = state.note_activity(timestamp) {
                out.push(boundary);
            }

            // Each command is reported twice (phase "start" then "end" with
            // the exit code). Only the "end" report carries the full picture,
            // so the "start" report is just a heads-up and is dropped here to
            // avoid emitting two entries per command.
            if phase == Some("start") {
                return out;
            }

            // Up-arrow-and-rerun spam: same command + cwd in quick succession.
            if state.is_repeated_command(&command, &cwd, timestamp) {
                state.record_command(&command, &cwd, timestamp);
                return out;
            }
            state.record_command(&command, &cwd, timestamp);
            state.note_on_task_activity();

            let parsed = parse_command(&command, &cwd);

            if let Some(project) = &parsed.project {
                if let Some(boundary) = state.note_project(project, timestamp) {
                    out.push(boundary);
                }
            }

            if is_test_command(&parsed, &command) && exit_code.map(|c| c != 0).unwrap_or(false) {
                state.record_test_failure(timestamp);
            }

            let summary = match exit_code {
                Some(code) => format!("Ran `{}` (exit {})", command, code),
                None => format!("Ran `{}`", command),
            };

            let is_commit = is_git_commit(&parsed) && exit_code.map(|c| c == 0).unwrap_or(true);

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "terminal".to_string(),
                kind: "command".to_string(),
                summary,
                entities: Entities {
                    project: parsed.project,
                    branch: parsed.branch,
                    tool: parsed.tool,
                    subcommand: parsed.subcommand,
                    exit_code,
                    ..Default::default()
                },
                correlated_with: vec![],
            });

            if is_commit {
                out.push(state.git_commit_boundary(timestamp));
            }
        }

        "Browser" => {
            let url = inner.get("url").and_then(|v| v.as_str()).map(String::from);
            let site = inner.get("site").and_then(|v| v.as_str()).map(String::from);
            let title = inner.get("title").and_then(|v| v.as_str()).map(String::from);
            let category = inner.get("category").and_then(|v| v.as_str()).map(String::from);
            let search_query = inner.get("search_query").and_then(|v| v.as_str()).map(String::from);

            if let Some(boundary) = state.note_activity(timestamp) {
                out.push(boundary);
            }

            if let Some(boundary) = state.note_browser_category(category.as_deref(), timestamp) {
                out.push(boundary);
            }

            let domain = url.as_deref().and_then(extract_domain).or_else(|| site.clone());

            let mut correlated_with = Vec::new();
            if search_query.is_some() && state.check_test_failure_correlation(timestamp) {
                correlated_with.push("recent_test_failure".to_string());
            }

            let summary = match (&search_query, &title, &domain) {
                (Some(q), _, _) => format!("Searched \"{}\"", q),
                (None, Some(t), Some(d)) => format!("Visited {} ({})", d, t),
                (None, _, Some(d)) => format!("Visited {}", d),
                _ => "Browser navigation".to_string(),
            };

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "browser".to_string(),
                kind: "navigation".to_string(),
                summary,
                entities: Entities {
                    domain,
                    category,
                    search_query,
                    project: state.current_project(),
                    ..Default::default()
                },
                correlated_with,
            });
        }

        "Clipboard" => {
            let content_type = inner.get("content_type").and_then(|v| v.as_str()).unwrap_or("plain_text").to_string();
            let length = inner.get("length").and_then(|v| v.as_u64()).unwrap_or(0);

            if let Some(boundary) = state.note_activity(timestamp) {
                out.push(boundary);
            }

            let summary = format!("Copied {} ({} chars)", content_type, length);

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "clipboard".to_string(),
                kind: "clipboard".to_string(),
                summary,
                entities: Entities {
                    content_type: Some(content_type),
                    project: state.current_project(),
                    ..Default::default()
                },
                correlated_with: vec![],
            });
        }

        "FileChange" => {
            let path = inner.get("path").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let kind = inner.get("kind").and_then(|v| v.as_str()).unwrap_or("changed").to_string();
            let project = inner.get("project").and_then(|v| v.as_str()).map(String::from);

            if let Some(boundary) = state.note_activity(timestamp) {
                out.push(boundary);
            }
            state.note_on_task_activity();

            let file_name = Path::new(&path).file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| path.clone());

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "filesystem".to_string(),
                kind: "file_change".to_string(),
                summary: format!("File {}: {}", kind, file_name),
                entities: Entities {
                    file: Some(path),
                    project: project.or_else(|| state.current_project()),
                    ..Default::default()
                },
                correlated_with: vec![],
            });
        }

        "ProcessActivity" => {
            let listening_ports = inner.get("listening_ports").and_then(|v| v.as_array());
            let summary = match listening_ports {
                Some(ports) if !ports.is_empty() => {
                    let names: Vec<String> = ports
                        .iter()
                        .filter_map(|p| {
                            let process = p.get("process").and_then(|v| v.as_str())?;
                            let port = p.get("port").and_then(|v| v.as_u64())?;
                            Some(format!("{}:{}", process, port))
                        })
                        .collect();
                    format!("Listening: {}", names.join(", "))
                }
                _ => "No listening services".to_string(),
            };

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "process".to_string(),
                kind: "process_activity".to_string(),
                summary,
                entities: Entities { project: state.current_project(), ..Default::default() },
                correlated_with: vec![],
            });
        }

        "Calendar" => {
            let current = inner.get("current_event").filter(|v| !v.is_null());
            let next = inner.get("next_event").filter(|v| !v.is_null());

            let summary = if let Some(c) = current {
                format!("In progress: {}", c.get("summary").and_then(|v| v.as_str()).unwrap_or("event"))
            } else if let Some(n) = next {
                format!("Next up: {}", n.get("summary").and_then(|v| v.as_str()).unwrap_or("event"))
            } else {
                "No upcoming events".to_string()
            };

            out.push(NormalizedEvent {
                seq: state.next_seq(),
                timestamp,
                session_id: state.session_id,
                source: "calendar".to_string(),
                kind: "calendar".to_string(),
                summary,
                entities: Entities::default(),
                correlated_with: vec![],
            });
        }

        _ => {}
    }

    out
}

fn describe_window(parsed: &WindowEntities, title: &Option<String>) -> String {
    match (&parsed.app, &parsed.file, &parsed.project) {
        (Some(app), Some(file), Some(project)) => format!("{} ({} in {})", app, file, project),
        (Some(app), None, Some(project)) => format!("{} ({})", app, project),
        (Some(app), _, None) => app.clone(),
        _ => title.clone().unwrap_or_else(|| "window".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn browser_event(category: &str, timestamp: u64) -> Value {
        json!({
            "Browser": {
                "source": "extension",
                "site": "example.com",
                "url": "https://example.com",
                "title": "Example",
                "h1": null,
                "selection": null,
                "category": category,
                "search_query": null,
                "timestamp": timestamp,
            }
        })
    }

    #[test]
    fn sustained_off_task_browsing_starts_a_new_session() {
        let mut state = NormalizerState::new();
        let start_session = state.session_id;

        // A single off-task glance doesn't trigger anything.
        let out = process_event(&mut state, &browser_event("social", 1_000));
        assert!(out.iter().all(|e| e.kind != "session_boundary"));
        assert_eq!(state.session_id, start_session);

        // Still under the drift threshold.
        let out = process_event(&mut state, &browser_event("social", 1_060));
        assert!(out.iter().all(|e| e.kind != "session_boundary"));
        assert_eq!(state.session_id, start_session);

        // Past OFF_TASK_DRIFT_THRESHOLD_SECS (180s) of continuous off-task
        // browsing: a new session starts.
        let out = process_event(&mut state, &browser_event("social", 1_181));
        assert!(out.iter().any(|e| e.kind == "session_boundary" && e.entities.category.as_deref() == Some("off_task_drift")));
        assert_eq!(state.session_id, start_session + 1);

        // The boundary fires only once per off-task run.
        let out = process_event(&mut state, &browser_event("social", 1_300));
        assert!(out.iter().all(|e| e.kind != "session_boundary"));
        assert_eq!(state.session_id, start_session + 1);
    }

    #[test]
    fn on_task_browsing_does_not_trigger_drift_boundary() {
        let mut state = NormalizerState::new();
        let start_session = state.session_id;

        for ts in [1_000, 1_100, 1_200, 1_300, 1_400] {
            let out = process_event(&mut state, &browser_event("documentation", ts));
            assert!(out.iter().all(|e| e.kind != "session_boundary"));
        }
        assert_eq!(state.session_id, start_session);
    }

    #[test]
    fn returning_to_work_before_threshold_resets_drift_run() {
        let mut state = NormalizerState::new();
        let start_session = state.session_id;

        process_event(&mut state, &browser_event("social", 1_000));
        process_event(&mut state, &browser_event("social", 1_060));

        // Switch back to documentation before the 180s threshold elapses.
        process_event(&mut state, &browser_event("documentation", 1_090));

        // More off-task browsing afterwards needs its own 180s run.
        let out = process_event(&mut state, &browser_event("social", 1_181));
        assert!(out.iter().all(|e| e.kind != "session_boundary"));
        assert_eq!(state.session_id, start_session);
    }
}
