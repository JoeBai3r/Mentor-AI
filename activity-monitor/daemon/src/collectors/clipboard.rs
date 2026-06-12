use crate::event_bus::{Event, SharedBus};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const PREVIEW_CHARS: usize = 200;

/// Polls the system clipboard for changes and publishes a classified
/// summary. Clipboard content is one of the highest-signal-density inputs:
/// a copied stack trace means "about to debug", a copied URL means
/// "referencing something", a copied code block means "about to adapt it".
pub fn start_clipboard_collector(bus: SharedBus) {
    std::thread::spawn(move || {
        let mut clipboard = match arboard::Clipboard::new() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("clipboard collector unavailable: {}", e);
                return;
            }
        };

        let mut last: Option<String> = None;
        loop {
            if let Ok(text) = clipboard.get_text() {
                if !text.trim().is_empty() && Some(&text) != last.as_ref() {
                    let ev = build_event(&text);
                    bus.publish_event(ev);
                    last = Some(text);
                }
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    });
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn build_event(text: &str) -> Event {
    let trimmed = text.trim();
    let length = trimmed.chars().count();
    let line_count = trimmed.lines().count();

    let (content_type, preview) = if looks_like_secret(trimmed) {
        ("secret".to_string(), None)
    } else {
        (classify(trimmed).to_string(), Some(truncate(trimmed)))
    };

    Event::Clipboard {
        content_type,
        preview,
        length,
        line_count,
        timestamp: now_ts(),
    }
}

fn truncate(s: &str) -> String {
    s.chars().take(PREVIEW_CHARS).collect()
}

/// Classify clipboard content so the consumer gets a clean intent signal
/// rather than raw text it then has to re-parse.
fn classify(s: &str) -> &'static str {
    let lower = s.to_lowercase();

    if (s.starts_with("http://") || s.starts_with("https://")) && !s.contains('\n') {
        return "url";
    }

    if lower.contains("traceback (most recent call last)")
        || lower.contains("panicked at")
        || lower.contains("exception in thread")
        || lower.contains("stack trace")
        || (lower.contains(" at ") && lower.lines().count() > 2)
    {
        return "stack_trace";
    }

    if (lower.contains("error") || lower.contains("exception") || lower.contains("fatal")) && s.lines().count() <= 5 {
        return "error_message";
    }

    const CODE_MARKERS: &[&str] = &[
        "fn ", "function ", "def ", "class ", "const ", "let ", "import ", "=>", "{", "};", "</", "#include", "select ", "return ",
    ];
    if CODE_MARKERS.iter().any(|m| lower.contains(m)) {
        return "code";
    }

    "plain_text"
}

/// Heuristic guard against capturing likely credentials/tokens: a single
/// "word" with no whitespace, reasonable token length, and mixed character
/// classes (lower+upper+digit) - typical of API keys, passwords, secrets.
fn looks_like_secret(s: &str) -> bool {
    if s.lines().count() != 1 || s.contains(char::is_whitespace) {
        return false;
    }
    let len = s.chars().count();
    if !(20..=256).contains(&len) {
        return false;
    }
    let has_lower = s.chars().any(|c| c.is_ascii_lowercase());
    let has_upper = s.chars().any(|c| c.is_ascii_uppercase());
    let has_digit = s.chars().any(|c| c.is_ascii_digit());
    has_lower && has_upper && has_digit
}
