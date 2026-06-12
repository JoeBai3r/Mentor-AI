//! The immediate intent layer ("immediate state builder") - the fastest
//! moving tier of the three-level intent system.
//!
//! Individual normalized events are nearly meaningless in isolation: a
//! single failing `pytest` invocation tells you nothing. Three failures in
//! nine minutes with interleaved browser searches tells you the user is
//! stuck. This module turns the live normalized event stream into that kind
//! of classified signal, in real time:
//!
//! 1. **Sliding window buffer** ([`buffer`]): a 5-minute, source-keyed view
//!    of recent normalized events.
//! 2. **Pattern recognizers** ([`recognizers`]): stateless functions that
//!    look for cross-source combinations - error loops, short-term command
//!    repetition, idle pauses, project/context switches.
//! 3. **Signal classification + blocking score** (this module): picks the
//!    highest-priority recognizer result as the dominant `signal`, and
//!    computes a continuous `blocking_score` from buffer-wide conditions.
//! 4. **Intervention gate** (this module): decides whether *now* is a good
//!    moment to surface anything, independent of how strong the signal is.
//!
//! An [`ImmediateState`] is emitted whenever the dominant signal, the gate,
//! or the blocking score change meaningfully (see [`should_emit`]). The
//! distiller ([`crate::distiller`]) consumes this stream alongside
//! normalized events to enrich each session's [`crate::distiller::SessionIntent`]
//! with what happened moment-to-moment, not just what happened in
//! aggregate.
//!
//! ## Known gaps
//!
//! The intervention gate's hard-close for "the user is actively typing"
//! isn't implemented: there is no per-keystroke signal anywhere in the
//! collector pipeline, and adding one would be a meaningfully different
//! (and more privacy-sensitive) kind of collector. "A command is currently
//! running", "in a call" (inferred from the focused window's app name), and
//! the cooldown are implemented. `last_gate_open_ts` stands in for "last
//! suggestion timestamp" until the recommendation engine's accept/dismiss
//! feedback is wired into `profile.behavioral.recommendation_accept_rate`.

mod buffer;
mod recognizers;

use crate::event_bus::SharedBus;
use crate::profile::UserProfile;
use buffer::SlidingWindowBuffer;
use recognizers::{detect_context_switch, detect_error_loop, detect_idle, detect_repetition};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// How far back the sliding window buffer looks.
pub const WINDOW_TTL_SECS: u64 = 300;
/// How long all "meaningful" sources must be quiet before `idle` fires.
pub const IDLE_THRESHOLD_SECS: u64 = 30;
/// A blocking-score change of at least this much (regardless of direction)
/// triggers an emission even if the signal and gate are unchanged.
pub const EMIT_SCORE_DELTA: f64 = 0.15;
/// How often to re-evaluate the buffer when no new event has arrived - the
/// only way `idle` and cooldown expiry are detected without new input.
const TICK_INTERVAL_SECS: u64 = 10;
/// Fallback used when the profile hasn't yet recorded a typical debugging
/// session length.
const DEFAULT_DEBUG_SESSION_MIN: f64 = 28.0;
/// `open:extended_block` fires once a current error loop has run longer than
/// this fraction of the user's typical debugging session.
const EXTENDED_BLOCK_FRACTION: f64 = 0.75;

/// Apps whose focus indicates the user is in a call - a hard close on the
/// intervention gate regardless of signal/score.
const CALL_APPS: &[&str] = &["zoom", "teams", "google meet", "discord", "slack huddle"];

/// How long a `phase: "start"` terminal event can suppress the gate via
/// `closed:command_running` before it's ignored. The shell's DEBUG trap
/// fires `phase: "start"` for every foreground command, including
/// long-running ones (dev servers, editors, REPLs) that may never send a
/// matching `phase: "end"` - without this cap, running one of those from a
/// hooked terminal would lock the gate closed for the rest of the session.
const COMMAND_RUNNING_TIMEOUT_SECS: u64 = 120;

/// The classified, point-in-time output of the immediate intent layer.
/// Emitted on signal change, gate transition, or a `blocking_score` jump of
/// at least [`EMIT_SCORE_DELTA`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImmediateState {
    pub ts: u64,
    /// The session this state belongs to (from the triggering normalized
    /// event's `session_id`), so the distiller can group emissions by
    /// session.
    pub session_id: u64,
    /// `"error_loop"`, `"context_switch"`, `"repetition"`, `"idle"`, or
    /// `"none"`.
    pub signal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_context: Option<ErrorContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition_context: Option<RepetitionContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_switch: Option<ContextSwitchContext>,
    pub blocking_score: f64,
    pub blocking_score_delta: f64,
    /// `"closed:<reason>"` or `"open:<reason>"`.
    pub gate: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gate_open_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorContext {
    pub tool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subcommand: Option<String>,
    pub failure_count: u64,
    pub search_count: u64,
    pub duration_sec: u64,
    /// Domain of the first search after the initial failure - the most
    /// likely place a knowledge-bridge recommendation should link to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepetitionContext {
    pub sequence: Vec<String>,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSwitchContext {
    pub from_project: Option<String>,
    pub to_project: Option<String>,
    /// True if the previous `ImmediateState`'s blocking score was above
    /// 0.4 - i.e. the switch happened while the user looked stuck, which is
    /// a much stronger state-preserver trigger than a clean switch after a
    /// commit.
    pub mid_task: bool,
}

/// Subscribes to the normalized event stream, maintains the sliding window
/// buffer, and emits [`ImmediateState`] updates on the bus's immediate
/// channel.
pub fn start_immediate_layer(bus: SharedBus) {
    // Subscribe synchronously so no normalized event published by the
    // normalizer (started earlier in main.rs) can be missed.
    let mut rx = bus.subscribe_normalized();
    // The normalizer drops `phase: "start"` terminal events (only "end"
    // reports, which carry the exit code, reach the normalized stream) -
    // subscribe to the raw bus separately to track whether a command is
    // currently running for the `closed:command_running` gate hard-close.
    let mut raw_rx = bus.subscribe();
    tokio::spawn(async move {
        let mut window = SlidingWindowBuffer::new(WINDOW_TTL_SECS);
        let mut previous: Option<ImmediateState> = None;
        let mut last_gate_open_ts: Option<u64> = None;
        let mut profile = crate::store::load_profile();
        // When the most recent terminal `phase: "start"` was seen, if its
        // matching `phase: "end"` hasn't arrived yet. Cleared on "end"; also
        // treated as stale (see `COMMAND_RUNNING_TIMEOUT_SECS`) once it's
        // been running "too long" to plausibly still be a gate-relevant
        // build/test rather than a long-lived foreground process.
        let mut command_running_since: Option<u64> = None;
        let mut tick = tokio::time::interval(Duration::from_secs(TICK_INTERVAL_SECS));

        loop {
            tokio::select! {
                ev = rx.recv() => {
                    match ev {
                        Ok(val) => {
                            let now = buffer::event_ts(&val).max(now_unix());
                            let session_id = val.get("session_id").and_then(|v| v.as_u64()).unwrap_or(0);
                            window.push(val, now);
                            let command_running = is_command_running(command_running_since, now);
                            process(&bus, &window, &mut previous, &mut last_gate_open_ts, &profile, now, session_id, command_running);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("immediate layer lagged: {} normalized events dropped", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                ev = raw_rx.recv() => {
                    match ev {
                        Ok(val) => {
                            if let Some(term) = val.get("Terminal") {
                                match term.get("phase").and_then(|p| p.as_str()) {
                                    Some("start") => command_running_since = term.get("timestamp").and_then(|t| t.as_u64()).or_else(|| Some(now_unix())),
                                    Some("end") => command_running_since = None,
                                    _ => {}
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("immediate layer lagged: {} raw events dropped", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = tick.tick() => {
                    let now = now_unix();
                    window.evict(now);
                    profile = crate::store::load_profile();
                    let session_id = previous.as_ref().map(|p| p.session_id).unwrap_or(0);
                    let command_running = is_command_running(command_running_since, now);
                    process(&bus, &window, &mut previous, &mut last_gate_open_ts, &profile, now, session_id, command_running);
                }
            }
        }
    });
}

fn now_unix() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

/// True if a terminal `phase: "start"` was seen without a matching `"end"`
/// yet, and it started recently enough to plausibly still be a foreground
/// build/test (see [`COMMAND_RUNNING_TIMEOUT_SECS`]).
fn is_command_running(command_running_since: Option<u64>, now: u64) -> bool {
    command_running_since.map_or(false, |since| now.saturating_sub(since) < COMMAND_RUNNING_TIMEOUT_SECS)
}

/// Stages 2-4: run the recognizers, classify the dominant signal, score
/// it, evaluate the gate, and emit if it changed meaningfully.
fn process(
    bus: &SharedBus,
    window: &SlidingWindowBuffer,
    previous: &mut Option<ImmediateState>,
    last_gate_open_ts: &mut Option<u64>,
    profile: &UserProfile,
    now: u64,
    session_id: u64,
    command_running: bool,
) {
    let error_loop = detect_error_loop(window);
    let repetition = detect_repetition(window);
    let idle = detect_idle(window, now, IDLE_THRESHOLD_SECS);
    let mut context_switch = detect_context_switch(window);

    // mid_task: was the situation already looking blocked (per the previous
    // emitted state) when this switch happened?
    if let Some(cs) = context_switch.as_mut() {
        let prev_score = previous.as_ref().map(|p| p.blocking_score).unwrap_or(0.0);
        cs.mid_task = prev_score > 0.4;
    }

    let (signal, dominant_duration_sec) = classify(&error_loop, &context_switch, &repetition, &idle);

    let blocking_score = compute_blocking_score(window, &error_loop, dominant_duration_sec);
    let prev_score = previous.as_ref().map(|p| p.blocking_score).unwrap_or(0.0);
    let blocking_score_delta = blocking_score - prev_score;

    let gate = evaluate_gate(signal, &error_loop, &idle, window, now, *last_gate_open_ts, profile, command_running);

    let prev_gate_open = previous.as_ref().map(|p| p.gate.starts_with("open")).unwrap_or(false);
    let gate_open_now = gate.starts_with("open");
    let gate_open_at = if gate_open_now {
        if prev_gate_open { previous.as_ref().and_then(|p| p.gate_open_at) } else { Some(now) }
    } else {
        None
    };
    if gate_open_now && !prev_gate_open {
        *last_gate_open_ts = Some(now);
    }

    let current = ImmediateState {
        ts: now,
        session_id,
        signal: signal.to_string(),
        error_context: error_loop,
        repetition_context: repetition,
        context_switch,
        blocking_score,
        blocking_score_delta,
        gate,
        gate_open_at,
    };

    if should_emit(&current, previous) {
        if let Ok(v) = serde_json::to_value(&current) {
            bus.publish_immediate(v);
        }
        *previous = Some(current);
    } else if previous.is_some() {
        // Keep `ts`/score fresh for the next delta comparison even when we
        // don't emit, without resetting gate_open_at bookkeeping.
        let prev = previous.as_mut().unwrap();
        prev.ts = current.ts;
        prev.blocking_score = current.blocking_score;
        prev.blocking_score_delta = current.blocking_score_delta;
    } else {
        *previous = Some(current);
    }
}

/// Stage 3a: pick the highest-priority non-`None` recognizer result.
/// Priority (highest first): error loop, context switch, repetition, idle.
/// Returns the signal name and, for error-loop/idle, the duration that
/// feeds into the blocking score's duration factor.
fn classify(
    error_loop: &Option<ErrorContext>,
    context_switch: &Option<ContextSwitchContext>,
    repetition: &Option<RepetitionContext>,
    idle: &Option<u64>,
) -> (&'static str, Option<u64>) {
    if let Some(ctx) = error_loop {
        return ("error_loop", Some(ctx.duration_sec));
    }
    if context_switch.is_some() {
        return ("context_switch", None);
    }
    if repetition.is_some() {
        return ("repetition", None);
    }
    if let Some(idle_sec) = idle {
        return ("idle", Some(*idle_sec));
    }
    ("none", None)
}

/// Stage 3b: a continuous 0-1 score over buffer-wide conditions, independent
/// of which signal is dominant. Weights are starting points - per the
/// design, they should eventually be tuned per-user from
/// `profile.behavioral.recommendation_accept_rate` (if a user ignores
/// suggestions below 0.7, raise the thresholds for them). That recalibration
/// isn't wired up yet.
fn compute_blocking_score(window: &SlidingWindowBuffer, error_loop: &Option<ErrorContext>, dominant_duration_sec: Option<u64>) -> f64 {
    let mut score = 0.0;

    let terminal = window.by_source.get("terminal").map(Vec::as_slice).unwrap_or(&[]);
    let browser = window.by_source.get("browser").map(Vec::as_slice).unwrap_or(&[]);
    let filesystem = window.by_source.get("filesystem").map(Vec::as_slice).unwrap_or(&[]);

    // Failure rate in terminal commands.
    if !terminal.is_empty() {
        let failures = terminal
            .iter()
            .filter(|e| buffer::event_entities(e).get("exit_code").and_then(|v| v.as_i64()).map(|c| c != 0).unwrap_or(false))
            .count();
        let fail_rate = failures as f64 / terminal.len() as f64;
        score += fail_rate * 0.35;
    }

    // Search-to-edit ratio: lots of searching, few file saves.
    let searches = browser.iter().filter(|e| buffer::event_entities(e).get("search_query").and_then(|v| v.as_str()).is_some()).count();
    let file_saves = filesystem.len();
    if searches + file_saves > 0 {
        let search_ratio = searches as f64 / (searches + file_saves) as f64;
        score += search_ratio * 0.30;
    }

    // Duration of the dominant signal (longer = more blocked), capped at 10
    // minutes. Only error_loop carries this in practice (idle's "duration"
    // would otherwise inflate the score for someone who's simply stepped
    // away).
    if error_loop.is_some() {
        if let Some(duration_sec) = dominant_duration_sec {
            let duration_factor = (duration_sec as f64 / 600.0).min(1.0);
            score += duration_factor * 0.25;
        }
    }

    // Same-domain search repetition.
    let domains: Vec<&str> = browser.iter().filter_map(|e| buffer::event_entities(e).get("domain").and_then(|v| v.as_str())).collect();
    if !domains.is_empty() {
        let mut counts: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
        for d in &domains {
            *counts.entry(d).or_insert(0) += 1;
        }
        let top = counts.values().max().copied().unwrap_or(0);
        score += (top as f64 / domains.len() as f64) * 0.10;
    }

    score.min(1.0)
}

/// Stage 4: the intervention gate. A high blocking score doesn't mean
/// "interrupt now" - it means there's something worth surfacing *when the
/// moment is right*. This decides whether that moment has arrived.
fn evaluate_gate(
    signal: &str,
    error_loop: &Option<ErrorContext>,
    idle: &Option<u64>,
    window: &SlidingWindowBuffer,
    now: u64,
    last_gate_open_ts: Option<u64>,
    profile: &UserProfile,
    command_running: bool,
) -> String {
    if is_in_call(window) {
        return "closed:in_call".to_string();
    }

    if command_running {
        return "closed:command_running".to_string();
    }

    let cooldown_secs = (profile.rhythm.interruption_gap_threshold_min as u64) * 60;
    if let Some(last) = last_gate_open_ts {
        if now.saturating_sub(last) < cooldown_secs {
            return "closed:cooldown".to_string();
        }
    }

    if idle.is_some() {
        return "open:idle".to_string();
    }

    if signal == "context_switch" {
        return "open:context_switch".to_string();
    }

    if signal == "error_loop" {
        if let Some(ctx) = error_loop {
            let typical_debug_min = profile.rhythm.typical_phase_durations.get("debugging").map(|d| d.avg_min).unwrap_or(DEFAULT_DEBUG_SESSION_MIN);
            let threshold_sec = typical_debug_min * 60.0 * EXTENDED_BLOCK_FRACTION;
            if ctx.duration_sec as f64 > threshold_sec {
                return "open:extended_block".to_string();
            }
        }
    }

    "closed:no_trigger".to_string()
}

/// True if the most recently focused window (within the buffer) is a known
/// video-call app.
fn is_in_call(window: &SlidingWindowBuffer) -> bool {
    let windows = window.by_source.get("window").map(Vec::as_slice).unwrap_or(&[]);
    windows.iter().rev().find_map(|e| buffer::event_entities(e).get("app").and_then(|v| v.as_str())).map(|app| {
        let lower = app.to_lowercase();
        CALL_APPS.iter().any(|c| lower.contains(c))
    }).unwrap_or(false)
}

/// Only emit when something meaningfully changed - prevents the session
/// ring buffer from flooding on near-identical states.
pub fn should_emit(current: &ImmediateState, previous: &Option<ImmediateState>) -> bool {
    match previous {
        None => true,
        Some(prev) => current.signal != prev.signal || current.gate != prev.gate || (current.blocking_score - prev.blocking_score).abs() >= EMIT_SCORE_DELTA,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn ev(seq: u64, ts: u64, session_id: u64, source: &str, kind: &str, entities: Value) -> Value {
        json!({"seq": seq, "timestamp": ts, "session_id": session_id, "source": source, "kind": kind, "summary": "", "entities": entities})
    }

    #[test]
    fn detects_error_loop_and_opens_gate_on_extended_block() {
        let mut window = SlidingWindowBuffer::new(WINDOW_TTL_SECS);
        let base = 1_000u64;

        // First failure.
        window.push(ev(1, base, 1, "terminal", "command", json!({"tool": "pytest", "subcommand": "test", "exit_code": 1})), base);
        // Search after the failure.
        window.push(ev(2, base + 60, 1, "browser", "navigation", json!({"domain": "stackoverflow.com", "search_query": "IntegrityError"})), base + 60);
        // Retry, also failing, 150s after the first failure (within the
        // 5-minute window).
        let retry_ts = base + 150;
        window.push(ev(3, retry_ts, 1, "terminal", "command", json!({"tool": "pytest", "subcommand": "test", "exit_code": 1})), retry_ts);

        let error_loop = detect_error_loop(&window).expect("error loop detected");
        assert_eq!(error_loop.tool, "pytest");
        assert_eq!(error_loop.failure_count, 2);
        assert_eq!(error_loop.search_count, 1);
        assert_eq!(error_loop.duration_sec, 150);
        assert_eq!(error_loop.domain.as_deref(), Some("stackoverflow.com"));

        let (signal, duration) = classify(&Some(error_loop.clone()), &None, &None, &None);
        assert_eq!(signal, "error_loop");
        assert_eq!(duration, Some(150));

        // Give this user a 2-minute typical debugging session: 75% of that
        // is 90s. 150s > 90s, so the gate should open on extended_block.
        let mut profile = UserProfile::seed_typical_developer();
        profile.rhythm.typical_phase_durations.insert("debugging".to_string(), crate::profile::PhaseDuration { avg_min: 2.0, resolves_within_session: 0.6 });
        let gate = evaluate_gate(signal, &Some(error_loop), &None, &window, retry_ts, None, &profile, false);
        assert_eq!(gate, "open:extended_block");
    }

    #[test]
    fn idle_opens_gate_after_threshold() {
        let mut window = SlidingWindowBuffer::new(WINDOW_TTL_SECS);
        let base = 1_000u64;
        window.push(ev(1, base, 1, "terminal", "command", json!({"tool": "cargo", "subcommand": "build", "exit_code": 0})), base);

        let now = base + IDLE_THRESHOLD_SECS + 5;
        let idle = detect_idle(&window, now, IDLE_THRESHOLD_SECS);
        assert_eq!(idle, Some(IDLE_THRESHOLD_SECS + 5));

        let profile = UserProfile::seed_typical_developer();
        let gate = evaluate_gate("idle", &None, &idle, &window, now, None, &profile, false);
        assert_eq!(gate, "open:idle");
    }

    #[test]
    fn cooldown_closes_gate_after_recent_open() {
        let window = SlidingWindowBuffer::new(WINDOW_TTL_SECS);
        let profile = UserProfile::seed_typical_developer();
        let now = 10_000u64;
        // interruption_gap_threshold_min defaults to 5 -> 300s cooldown.
        let gate = evaluate_gate("idle", &None, &Some(40), &window, now, Some(now - 60), &profile, false);
        assert_eq!(gate, "closed:cooldown");
    }

    #[test]
    fn command_running_closes_gate_even_when_idle() {
        let window = SlidingWindowBuffer::new(WINDOW_TTL_SECS);
        let profile = UserProfile::seed_typical_developer();
        let now = 10_000u64;
        // Without a running command, idle would open the gate...
        let gate = evaluate_gate("idle", &None, &Some(40), &window, now, None, &profile, false);
        assert_eq!(gate, "open:idle");
        // ...but a long-running build/test command suppresses the
        // interruption even though the user appears idle.
        let gate = evaluate_gate("idle", &None, &Some(40), &window, now, None, &profile, true);
        assert_eq!(gate, "closed:command_running");
    }

    #[test]
    fn should_emit_on_score_jump_and_signal_change() {
        let prev = ImmediateState { signal: "none".to_string(), gate: "closed:no_trigger".to_string(), blocking_score: 0.1, ..Default::default() };

        let same = ImmediateState { signal: "none".to_string(), gate: "closed:no_trigger".to_string(), blocking_score: 0.15, ..Default::default() };
        assert!(!should_emit(&same, &Some(prev.clone())));

        let big_jump = ImmediateState { signal: "none".to_string(), gate: "closed:no_trigger".to_string(), blocking_score: 0.3, ..Default::default() };
        assert!(should_emit(&big_jump, &Some(prev.clone())));

        let signal_change = ImmediateState { signal: "error_loop".to_string(), gate: "closed:no_trigger".to_string(), blocking_score: 0.1, ..Default::default() };
        assert!(should_emit(&signal_change, &Some(prev)));
    }
}
