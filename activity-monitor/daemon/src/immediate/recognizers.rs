//! Stage 2: pattern recognizers.
//!
//! Each recognizer is a stateless function over the current buffer
//! contents, called on every push (and on the periodic tick). They look at
//! combinations across sources, not single-source streams - that's what
//! lets `detect_error_loop` see "failure, then search, then retry" as one
//! pattern instead of three unrelated events.

use super::buffer::{event_entities, event_ts, SlidingWindowBuffer};
use super::{ContextSwitchContext, ErrorContext, RepetitionContext};
use serde_json::Value;
use std::collections::HashMap;

/// A repeated `seq_len`-command subsequence appearing this many times within
/// the window is "repetition" (short-term, distinct from the profile's
/// long-term `friction.command_sequences`).
const REPETITION_THRESHOLD: u32 = 3;
const REPETITION_SEQ_LENS: &[usize] = &[2, 3, 4];

fn str_field<'a>(entities: &'a Value, key: &str) -> Option<&'a str> {
    entities.get(key).and_then(|v| v.as_str())
}

fn is_failure(event: &Value) -> bool {
    event_entities(event).get("exit_code").and_then(|v| v.as_i64()).map(|c| c != 0).unwrap_or(false)
}

fn is_search(event: &Value) -> bool {
    event_entities(event).get("search_query").and_then(|v| v.as_str()).is_some()
}

/// `"{tool} {subcommand}"` (or just `"{tool}"`) - the same token format the
/// session summarizer uses for `SessionIntent::command_sequence`, so the
/// short-term repetition signal here and the long-term pattern accumulator
/// in `crate::distiller::aggregate` recognize the same n-grams.
fn command_token(event: &Value) -> Option<String> {
    let entities = event_entities(event);
    let tool = str_field(entities, "tool")?;
    Some(match str_field(entities, "subcommand") {
        Some(sub) => format!("{} {}", tool, sub),
        None => tool.to_string(),
    })
}

/// The error-loop recognizer: failure -> interleaved search -> retry of the
/// same tool. Requires at least two failures, at least one search after the
/// first failure, and at least one of those failures (a "retry") after the
/// first such search.
pub fn detect_error_loop(buffer: &SlidingWindowBuffer) -> Option<ErrorContext> {
    let terminal = buffer.by_source.get("terminal").map(Vec::as_slice).unwrap_or(&[]);
    let browser = buffer.by_source.get("browser").map(Vec::as_slice).unwrap_or(&[]);

    let failures: Vec<&Value> = terminal.iter().filter(|e| is_failure(e)).collect();
    if failures.len() < 2 {
        return None;
    }

    let first_fail = failures[0];
    let first_fail_ts = event_ts(first_fail);
    let tool = str_field(event_entities(first_fail), "tool")?.to_string();
    let subcommand = str_field(event_entities(first_fail), "subcommand").map(String::from);

    let searches_after: Vec<&Value> = browser.iter().filter(|e| event_ts(e) > first_fail_ts && is_search(e)).collect();
    if searches_after.is_empty() {
        return None;
    }
    let first_search_ts = event_ts(searches_after[0]);

    let retries = failures
        .iter()
        .filter(|e| event_ts(e) > first_search_ts && str_field(event_entities(e), "tool") == Some(tool.as_str()))
        .count();
    if retries == 0 {
        return None;
    }

    let last_fail_ts = event_ts(failures.last().unwrap());
    let domain = searches_after.iter().find_map(|e| str_field(event_entities(e), "domain")).map(String::from);

    Some(ErrorContext {
        tool,
        subcommand,
        failure_count: failures.len() as u64,
        search_count: searches_after.len() as u64,
        duration_sec: last_fail_ts.saturating_sub(first_fail_ts),
        domain,
    })
}

/// The repetition recognizer: a 2-4 command subsequence repeated 3+ times
/// within the window - e.g. edit/build/fail looping without an error-loop's
/// search step.
pub fn detect_repetition(buffer: &SlidingWindowBuffer) -> Option<RepetitionContext> {
    let terminal = buffer.by_source.get("terminal").map(Vec::as_slice).unwrap_or(&[]);
    let commands: Vec<String> = terminal.iter().filter_map(command_token).collect();

    for &seq_len in REPETITION_SEQ_LENS {
        if commands.len() < seq_len {
            continue;
        }
        let mut counts: HashMap<&[String], u32> = HashMap::new();
        for window in commands.windows(seq_len) {
            *counts.entry(window).or_insert(0) += 1;
        }
        if let Some((seq, count)) = counts.into_iter().filter(|(_, c)| *c >= REPETITION_THRESHOLD).max_by_key(|(_, c)| *c) {
            return Some(RepetitionContext { sequence: seq.to_vec(), count });
        }
    }
    None
}

/// The idle recognizer: how long it's been since the last *meaningful*
/// event (terminal/browser/filesystem/clipboard). Window focus churn from
/// background polling doesn't count - without this distinction the system
/// never detects idle, since the window collector fires continuously.
///
/// Returns `Some(idle_seconds)` if idle for at least `threshold_sec`.
pub fn detect_idle(buffer: &SlidingWindowBuffer, now: u64, threshold_sec: u64) -> Option<u64> {
    if buffer.events.is_empty() {
        return Some(threshold_sec);
    }

    const MEANINGFUL: &[&str] = &["terminal", "browser", "filesystem", "clipboard"];
    let last_meaningful_ts = buffer
        .events
        .iter()
        .filter(|e| MEANINGFUL.contains(&e.get("source").and_then(|v| v.as_str()).unwrap_or("")))
        .map(event_ts)
        .max();

    match last_meaningful_ts {
        Some(last) => {
            let idle = now.saturating_sub(last);
            if idle >= threshold_sec { Some(idle) } else { None }
        }
        // Nothing but background polling in the window: treat the age of
        // the most recent event as the idle duration.
        None => Some(now.saturating_sub(event_ts(buffer.events.last().unwrap()))),
    }
}

/// The context-switch recognizer: compares the project referenced by the
/// last 5 events to the project referenced by everything before that.
/// `mid_task` is left for the caller to fill in (it depends on the running
/// blocking score, which the recognizer doesn't have access to).
pub fn detect_context_switch(buffer: &SlidingWindowBuffer) -> Option<ContextSwitchContext> {
    let n = buffer.events.len();
    if n < 10 {
        return None;
    }
    let recent = &buffer.events[n - 5..];
    let earlier = &buffer.events[..n - 5];

    let recent_project = extract_project(recent)?;
    let earlier_project = extract_project(earlier)?;

    if recent_project == earlier_project {
        return None;
    }

    Some(ContextSwitchContext { from_project: Some(earlier_project), to_project: Some(recent_project), mid_task: false })
}

fn extract_project(events: &[Value]) -> Option<String> {
    events.iter().rev().find_map(|e| str_field(event_entities(e), "project")).map(String::from)
}
