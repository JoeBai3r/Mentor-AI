use crate::event_bus::{CalendarEvent, Event, SharedBus};
use chrono::{DateTime, Duration as ChronoDuration, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const POLL_INTERVAL: Duration = Duration::from_secs(300);

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

struct ParsedEvent {
    summary: String,
    start: DateTime<Utc>,
    end_explicit: Option<DateTime<Utc>>,
    all_day: bool,
}

impl ParsedEvent {
    fn effective_end(&self) -> DateTime<Utc> {
        if let Some(e) = self.end_explicit {
            e
        } else if self.all_day {
            self.start + ChronoDuration::days(1)
        } else {
            self.start
        }
    }

    fn to_calendar_event(&self) -> CalendarEvent {
        CalendarEvent {
            summary: self.summary.clone(),
            start: self.start.to_rfc3339(),
            end: self.end_explicit.map(|d| d.to_rfc3339()),
        }
    }
}

/// Reads `.ics` files from `ACTIVITY_MONITOR_CALENDAR_DIR` and publishes the
/// current and next calendar events. This is the only collector that gives
/// forward-looking context: an upcoming "sprint planning" suggests prep
/// mode, a just-finished "1:1 with manager" suggests new tasks may follow.
/// Disables itself gracefully if the env var is unset or invalid.
pub fn start_calendar_collector(bus: SharedBus) {
    let dir = match std::env::var("ACTIVITY_MONITOR_CALENDAR_DIR") {
        Ok(d) if Path::new(&d).is_dir() => d,
        Ok(d) => {
            tracing::info!("calendar collector disabled: {} is not a directory", d);
            return;
        }
        Err(_) => {
            tracing::info!("calendar collector disabled: ACTIVITY_MONITOR_CALENDAR_DIR not set");
            return;
        }
    };

    tokio::spawn(async move {
        let mut last_key: Option<(Option<String>, Option<String>)> = None;
        loop {
            let events = load_events(&dir);
            let now = Utc::now();

            let current = events.iter().find(|e| e.start <= now && now < e.effective_end());
            let next = events.iter().filter(|e| e.start > now).min_by_key(|e| e.start);

            let current = current.map(|e| e.to_calendar_event());
            let next = next.map(|e| e.to_calendar_event());

            let key = (
                current.as_ref().map(|e| format!("{}@{}", e.summary, e.start)),
                next.as_ref().map(|e| format!("{}@{}", e.summary, e.start)),
            );

            if Some(&key) != last_key.as_ref() {
                last_key = Some(key);
                bus.publish_event(Event::Calendar {
                    current_event: current,
                    next_event: next,
                    timestamp: now_ts(),
                });
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}

fn load_events(dir: &str) -> Vec<ParsedEvent> {
    let mut events = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return events,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("ics") {
            continue;
        }
        if let Ok(content) = std::fs::read_to_string(&path) {
            events.extend(parse_ics(&content));
        }
    }

    events.sort_by_key(|e| e.start);
    events
}

/// Joins ICS line-folded continuations (lines starting with a space or tab
/// continue the previous line) into single logical lines.
fn unfold(content: &str) -> String {
    let mut out = String::new();
    for line in content.lines() {
        if (line.starts_with(' ') || line.starts_with('\t')) && !out.is_empty() {
            out.push_str(&line[1..]);
        } else {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(line);
        }
    }
    out
}

fn split_ics_line(line: &str) -> Option<(String, String)> {
    let idx = line.find(':')?;
    let key_full = &line[..idx];
    let value = &line[idx + 1..];
    let key = key_full.split(';').next().unwrap_or(key_full).to_string();
    Some((key, value.to_string()))
}

fn unescape(s: &str) -> String {
    s.replace("\\n", " ").replace("\\N", " ").replace("\\,", ",").replace("\\;", ";").replace("\\\\", "\\")
}

fn parse_ics(content: &str) -> Vec<ParsedEvent> {
    let unfolded = unfold(content);
    let mut events = Vec::new();
    let mut in_event = false;
    let mut summary: Option<String> = None;
    let mut dtstart: Option<(DateTime<Utc>, bool)> = None;
    let mut dtend: Option<DateTime<Utc>> = None;

    for line in unfolded.lines() {
        let line = line.trim_end_matches('\r');
        match line {
            "BEGIN:VEVENT" => {
                in_event = true;
                summary = None;
                dtstart = None;
                dtend = None;
                continue;
            }
            "END:VEVENT" => {
                if let (Some(s), Some((start, all_day))) = (summary.take(), dtstart.take()) {
                    events.push(ParsedEvent {
                        summary: s,
                        start,
                        end_explicit: dtend.take(),
                        all_day,
                    });
                }
                in_event = false;
                continue;
            }
            _ => {}
        }

        if !in_event {
            continue;
        }

        if let Some((key, value)) = split_ics_line(line) {
            match key.as_str() {
                "SUMMARY" => summary = Some(unescape(&value)),
                "DTSTART" => dtstart = parse_dt(&value),
                "DTEND" => dtend = parse_dt(&value).map(|(dt, _)| dt),
                _ => {}
            }
        }
    }

    events
}

/// Parses an ICS DATE or DATE-TIME value, returning (UTC datetime, is_all_day).
fn parse_dt(value: &str) -> Option<(DateTime<Utc>, bool)> {
    let value = value.trim();

    // All-day event: "YYYYMMDD"
    if value.len() == 8 && value.chars().all(|c| c.is_ascii_digit()) {
        let date = NaiveDate::parse_from_str(value, "%Y%m%d").ok()?;
        let dt = date.and_hms_opt(0, 0, 0)?;
        return Some((Utc.from_utc_datetime(&dt), true));
    }

    // UTC datetime: "YYYYMMDDTHHMMSSZ"
    if let Some(stripped) = value.strip_suffix('Z') {
        let ndt = NaiveDateTime::parse_from_str(stripped, "%Y%m%dT%H%M%S").ok()?;
        return Some((Utc.from_utc_datetime(&ndt), false));
    }

    // Floating/local datetime: "YYYYMMDDTHHMMSS"
    let ndt = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S").ok()?;
    let local = Local.from_local_datetime(&ndt).single()?;
    Some((local.with_timezone(&Utc), false))
}
