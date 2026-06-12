//! The recommendation engine: the action-taking tier above the three-level
//! intent system.
//!
//! Whenever the immediate intent layer's intervention gate transitions from
//! `closed:*` to `open:*` ([`crate::immediate`]), this module assembles a
//! prompt from that moment's [`crate::immediate::ImmediateState`] plus the
//! relevant slices of the long-term [`crate::profile::UserProfile`], asks
//! Claude (Sonnet 4.6) for a single short suggestion, and publishes the
//! result as a [`Recommendation`] on the bus and to SQLite.
//!
//! Accept/dismiss feedback ([`apply_feedback`]) is the one place outside the
//! distiller that writes to the profile directly: it nudges
//! `behavioral.recommendation_accept_rate[rec_type]` with the same EMA the
//! distiller uses elsewhere ([`crate::distiller::ema`]).
//!
//! If `ANTHROPIC_API_KEY` isn't set in the environment, a templated fallback
//! suggestion is used instead of a model call - the rest of the pipeline
//! (storage, publishing, accept/dismiss feedback) works the same either way.

use crate::event_bus::SharedBus;
use crate::profile::UserProfile;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default model, used when `ANTHROPIC_MODEL` isn't set in the environment.
const DEFAULT_CLAUDE_MODEL: &str = "claude-sonnet-4-6";
const CLAUDE_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// A single actionable suggestion generated when the intervention gate opens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    pub id: i64,
    pub ts: u64,
    pub session_id: u64,
    /// The dominant signal that triggered this recommendation
    /// (`"error_loop"`, `"context_switch"`, `"repetition"`, `"idle"`).
    pub signal: String,
    /// The gate string that triggered this recommendation (e.g.
    /// `"open:extended_block"`).
    pub gate: String,
    /// One of `"friction_remover"`, `"knowledge_bridge"`,
    /// `"workflow_accelerator"`, `"state_preserver"` - matches the keys in
    /// `profile.behavioral.recommendation_accept_rate`.
    pub rec_type: String,
    pub text: String,
    /// `"pending"`, `"accepted"`, or `"dismissed"`.
    pub status: String,
}

/// Subscribes to the immediate-state stream and generates a recommendation
/// every time the intervention gate transitions from closed to open.
pub fn start_recommendation_engine(bus: SharedBus) {
    let mut rx = bus.subscribe_immediate();
    let api_key = std::env::var("ANTHROPIC_API_KEY").ok();
    let model = std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| DEFAULT_CLAUDE_MODEL.to_string());
    if api_key.is_none() {
        tracing::warn!("ANTHROPIC_API_KEY not set - recommendations will use templated fallback text instead of calling Claude");
    } else {
        tracing::info!("recommendation engine using Claude model: {}", model);
    }

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut gate_was_open = false;

        loop {
            match rx.recv().await {
                Ok(val) => {
                    let gate = val.get("gate").and_then(|g| g.as_str()).unwrap_or("");
                    let gate_open = gate.starts_with("open");
                    if gate_open && !gate_was_open {
                        handle_gate_open(&bus, &client, api_key.as_deref(), &model, &val).await;
                    }
                    gate_was_open = gate_open;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("recommendation engine lagged: {} immediate states dropped", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

async fn handle_gate_open(bus: &SharedBus, client: &reqwest::Client, api_key: Option<&str>, model: &str, state: &Value) {
    let profile = crate::store::load_profile();
    let signal = state.get("signal").and_then(|v| v.as_str()).unwrap_or("none").to_string();
    let gate = state.get("gate").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let session_id = state.get("session_id").and_then(|v| v.as_u64()).unwrap_or(0);
    let ts = state.get("ts").and_then(|v| v.as_u64()).unwrap_or_else(now_unix);
    let rec_type = classify_rec_type(&signal, state);

    let text = match api_key {
        Some(key) => match call_claude(client, key, model, &signal, state, &profile, &rec_type).await {
            Ok(text) => text,
            Err(e) => {
                tracing::error!("Claude API call failed, using fallback suggestion: {}", e);
                fallback_text(&signal, state, &rec_type)
            }
        },
        None => fallback_text(&signal, state, &rec_type),
    };

    let mut rec = Recommendation {
        id: 0,
        ts,
        session_id,
        signal,
        gate,
        rec_type,
        text,
        status: "pending".to_string(),
    };

    match crate::store::store_recommendation(&rec) {
        Ok(id) => rec.id = id,
        Err(e) => {
            tracing::error!("failed to store recommendation: {}", e);
            return;
        }
    }

    if let Ok(v) = serde_json::to_value(&rec) {
        bus.publish_recommendation(v);
    }
}

/// Maps the dominant signal (and, for error loops, whether a follow-up
/// search domain was found) to one of the four recommendation types that
/// `profile.behavioral.recommendation_accept_rate` tracks.
fn classify_rec_type(signal: &str, state: &Value) -> String {
    match signal {
        "error_loop" => {
            let has_domain = state.get("error_context").and_then(|c| c.get("domain")).and_then(|d| d.as_str()).is_some();
            if has_domain { "knowledge_bridge" } else { "friction_remover" }
        }
        "repetition" => "workflow_accelerator",
        "context_switch" | "idle" => "state_preserver",
        _ => "friction_remover",
    }
    .to_string()
}

/// Builds the prompt and calls the Claude Messages API for a single short
/// suggestion.
async fn call_claude(client: &reqwest::Client, api_key: &str, model: &str, signal: &str, state: &Value, profile: &UserProfile, rec_type: &str) -> Result<String, String> {
    let prompt = build_prompt(signal, state, profile, rec_type);

    let body = serde_json::json!({
        "model": model,
        "max_tokens": 200,
        "messages": [{"role": "user", "content": prompt}],
    });

    let resp = client
        .post(CLAUDE_API_URL)
        .header("x-api-key", api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("{}: {}", status, text));
    }

    let json: Value = resp.json().await.map_err(|e| e.to_string())?;
    json.get("content")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.iter().find_map(|block| block.get("text").and_then(|t| t.as_str())))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("unexpected response shape: {}", json))
}

/// Assembles a prompt from the triggering [`crate::immediate::ImmediateState`]
/// and the profile slices each recommendation type cares about.
fn build_prompt(signal: &str, state: &Value, profile: &UserProfile, rec_type: &str) -> String {
    let mut details = String::new();

    match signal {
        "error_loop" => {
            if let Some(ctx) = state.get("error_context") {
                let tool = ctx.get("tool").and_then(|v| v.as_str()).unwrap_or("a tool");
                let failures = ctx.get("failure_count").and_then(|v| v.as_u64()).unwrap_or(0);
                let duration = ctx.get("duration_sec").and_then(|v| v.as_u64()).unwrap_or(0) / 60;
                details.push_str(&format!("The user has run `{}` and it has failed {} times over the last {} minutes.", tool, failures, duration.max(1)));
                if let Some(domain) = ctx.get("domain").and_then(|v| v.as_str()) {
                    details.push_str(&format!(" They've been searching {} for help with the error.", domain));
                }
            }
        }
        "repetition" => {
            if let Some(ctx) = state.get("repetition_context") {
                let count = ctx.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let seq: Vec<String> = ctx.get("sequence").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect()).unwrap_or_default();
                details.push_str(&format!("The user has repeated this sequence {} times: {}.", count, seq.join(" -> ")));
            }
        }
        "context_switch" => {
            if let Some(ctx) = state.get("context_switch") {
                let from = ctx.get("from_project").and_then(|v| v.as_str()).unwrap_or("their previous project");
                let to = ctx.get("to_project").and_then(|v| v.as_str()).unwrap_or("a new project");
                let mid_task = ctx.get("mid_task").and_then(|v| v.as_bool()).unwrap_or(false);
                details.push_str(&format!("The user is switching from {} to {}.", from, to));
                if mid_task {
                    details.push_str(" They appeared to be in the middle of debugging something blocked when the switch happened.");
                }
            }
        }
        "idle" => {
            details.push_str("The user has gone idle (no terminal, browser, editor, or filesystem activity) after a period of active work.");
        }
        _ => {}
    }

    let top_stack: Vec<String> = {
        let mut entries: Vec<(&String, &f64)> = profile.stack.stack_weights.iter().collect();
        entries.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
        entries.into_iter().take(3).map(|(k, _)| k.clone()).collect()
    };

    let accept_rate = profile.behavioral.recommendation_accept_rate.get(rec_type).copied().unwrap_or(0.5);

    format!(
        "You are an embedded productivity assistant observing a developer's live activity in real time. \
Based on the signal below, write ONE short, actionable suggestion for what they could do right now. \
Respond with the suggestion only - 1 to 3 sentences, no preamble, no markdown, no headers.\n\n\
Signal: {signal}\n\
{details}\n\n\
User profile:\n\
- Role: {role}\n\
- Primary stack: {stack}\n\
- Preferred suggestion style: {verbosity}\n\
- This user has historically accepted {accept_pct:.0}% of \"{rec_type}\"-type suggestions{accept_note}.",
        signal = signal,
        details = details,
        role = profile.identity.role_self_reported,
        stack = if top_stack.is_empty() { "unknown".to_string() } else { top_stack.join(", ") },
        verbosity = profile.behavioral.pref_recommendation_verbosity,
        accept_pct = accept_rate * 100.0,
        rec_type = rec_type,
        accept_note = if accept_rate < 0.35 { " - lean towards a lighter touch or skip the suggestion if it doesn't feel useful" } else { "" },
    )
}

/// A templated suggestion used when `ANTHROPIC_API_KEY` isn't set or the API
/// call fails - keeps the rest of the pipeline (storage, publishing,
/// accept/dismiss feedback) testable without a model call.
fn fallback_text(signal: &str, state: &Value, rec_type: &str) -> String {
    match signal {
        "error_loop" => {
            let tool = state.get("error_context").and_then(|c| c.get("tool")).and_then(|v| v.as_str()).unwrap_or("that command");
            if rec_type == "knowledge_bridge" {
                let domain = state.get("error_context").and_then(|c| c.get("domain")).and_then(|v| v.as_str()).unwrap_or("the docs");
                format!("You've been stuck on `{}` for a while and searching {} - want a hand digging into the root cause?", tool, domain)
            } else {
                format!("`{}` has failed a few times in a row - want to step through the error together?", tool)
            }
        }
        "repetition" => "You've repeated the same sequence of commands a few times - want help turning it into a script or alias?".to_string(),
        "context_switch" => "Looks like you're switching tasks - want a quick summary of where you left off so it's easy to pick back up later?".to_string(),
        "idle" => "You've been away for a bit - want a quick recap of what you were working on before the break?".to_string(),
        _ => "Now might be a good moment for a quick check-in - anything I can help with?".to_string(),
    }
}

/// Records the user's response to a recommendation: updates its `status` and
/// nudges `profile.behavioral.recommendation_accept_rate[rec_type]` with the
/// same EMA the distiller uses elsewhere. Returns the updated recommendation
/// and profile so the caller can republish both.
pub fn apply_feedback(id: i64, accepted: bool) -> Option<(Recommendation, UserProfile)> {
    let mut rec = crate::store::get_recommendation(id)?;
    let status = if accepted { "accepted" } else { "dismissed" };
    if let Err(e) = crate::store::update_recommendation_status(id, status) {
        tracing::error!("failed to update recommendation {}: {}", id, e);
    }
    rec.status = status.to_string();

    let mut profile = crate::store::load_profile();
    let sample = if accepted { 1.0 } else { 0.0 };
    let entry = profile.behavioral.recommendation_accept_rate.entry(rec.rec_type.clone()).or_insert(0.5);
    *entry = crate::distiller::ema(*entry, sample, crate::distiller::EMA_ALPHA);
    if let Err(e) = crate::store::save_profile(&profile) {
        tracing::error!("failed to save profile after recommendation feedback: {}", e);
    }

    Some((rec, profile))
}

fn now_unix() -> u64 {
    chrono::Utc::now().timestamp() as u64
}
