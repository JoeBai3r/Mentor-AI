use super::{Entities, NormalizedEvent};

/// A focused window/terminal/browser event more than this many seconds after
/// the previous one marks an inactivity-based session boundary.
pub const GAP_THRESHOLD_SECS: u64 = 300;

/// Window "blur" events shorter than this are alt-tab-and-back flicker, not
/// real focus changes, and are dropped.
pub const FLICKER_THRESHOLD_SECS: u64 = 1;

/// Repeating the exact same command + cwd within this window is treated as
/// up-arrow-and-rerun spam and only the first occurrence is kept.
pub const COMMAND_REPEAT_WINDOW_SECS: u64 = 5;

/// A browser search/navigation within this many seconds of a failing test
/// run is tagged as likely related to that failure.
pub const TEST_FAILURE_CORRELATION_WINDOW_SECS: u64 = 120;

/// Browser categories that represent off-task browsing (social media,
/// entertainment) rather than research/documentation related to the current
/// project.
const OFF_TASK_CATEGORIES: &[&str] = &["entertainment", "social"];

/// How long a continuous run of off-task browsing has to last before it's
/// treated as the start of a new session - the user has drifted away from
/// the project entirely, not just glanced at a tab.
pub const OFF_TASK_DRIFT_THRESHOLD_SECS: u64 = 180;

/// Apps that tend to run continuously in the background and whose focus
/// events carry little signal about what the user is working on.
const BACKGROUND_APPS: &[&str] = &["spotify", "nm-applet", "blueman-applet", "xfce4-power-manager", "indicator-application"];

pub fn is_background_app(application: &Option<String>) -> bool {
    match application {
        Some(app) => {
            let lower = app.to_lowercase();
            BACKGROUND_APPS.iter().any(|b| lower.contains(b))
        }
        None => false,
    }
}

/// Mutable state threaded through the normalization pipeline: tracks the
/// current session, the inferred "current project", and bookkeeping needed
/// for noise suppression and cross-source correlation.
pub struct NormalizerState {
    seq: u64,
    pub session_id: u64,
    last_activity_ts: Option<u64>,
    current_project: Option<String>,
    last_command: Option<(String, Option<String>, u64)>,
    last_test_failure_ts: Option<u64>,
    /// When the current run of off-task browsing (entertainment/social)
    /// started, if any. Reset to `None` by any on-task activity.
    off_task_since: Option<u64>,
    /// True once the current off-task run has already triggered a session
    /// boundary, so it isn't fired again every subsequent off-task event.
    off_task_boundary_fired: bool,
}

impl NormalizerState {
    pub fn new() -> Self {
        Self {
            seq: 0,
            session_id: 1,
            last_activity_ts: None,
            current_project: None,
            last_command: None,
            last_test_failure_ts: None,
            off_task_since: None,
            off_task_boundary_fired: false,
        }
    }

    pub fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn session_boundary(&mut self, timestamp: u64, reason: &str, summary: String) -> NormalizedEvent {
        self.session_id += 1;
        NormalizedEvent {
            seq: self.next_seq(),
            timestamp,
            session_id: self.session_id,
            source: "session".to_string(),
            kind: "session_boundary".to_string(),
            summary,
            entities: Entities { category: Some(reason.to_string()), ..Default::default() },
            correlated_with: vec![],
        }
    }

    /// Gap-based session boundary (Stage 2): returns `Some(event)` if too
    /// much time has passed since the last recorded user activity.
    pub fn note_activity(&mut self, timestamp: u64) -> Option<NormalizedEvent> {
        let boundary = match self.last_activity_ts {
            Some(prev) if timestamp > prev && timestamp - prev > GAP_THRESHOLD_SECS => {
                let gap_mins = (timestamp - prev) / 60;
                Some(self.session_boundary(timestamp, "inactivity_gap", format!("Inactive for {} min", gap_mins)))
            }
            _ => None,
        };
        if self.last_activity_ts.map_or(true, |prev| timestamp > prev) {
            self.last_activity_ts = Some(timestamp);
        }
        boundary
    }

    /// Semantic session boundary (Stage 2): returns `Some(event)` if the
    /// inferred "current project" changed since the last activity.
    pub fn note_project(&mut self, project: &str, timestamp: u64) -> Option<NormalizedEvent> {
        if self.current_project.as_deref() == Some(project) {
            return None;
        }
        let prev = self.current_project.replace(project.to_string());
        prev.map(|prev| self.session_boundary(timestamp, "project_switch", format!("Switched project: {} -> {}", prev, project)))
    }

    /// Semantic session boundary (Stage 2): a `git commit` marks the end of
    /// a unit of work regardless of the inactivity gap.
    pub fn git_commit_boundary(&mut self, timestamp: u64) -> NormalizedEvent {
        self.session_boundary(timestamp, "git_commit", "Committed changes".to_string())
    }

    /// Semantic session boundary (Stage 2): the user has been browsing
    /// off-task content (social media, entertainment) for at least
    /// [`OFF_TASK_DRIFT_THRESHOLD_SECS`]. Even if the gaps between individual
    /// navigations are short enough to not trip the inactivity-gap boundary,
    /// a sustained run of unrelated browsing means whatever comes next is a
    /// new unit of work, not a continuation of the current project session.
    ///
    /// Returns `Some(event)` at most once per off-task run (when the
    /// threshold is first crossed); any on-task activity
    /// ([`Self::note_on_task_activity`]) resets the run.
    pub fn note_browser_category(&mut self, category: Option<&str>, timestamp: u64) -> Option<NormalizedEvent> {
        let is_off_task = category.map(|c| OFF_TASK_CATEGORIES.contains(&c)).unwrap_or(false);

        if !is_off_task {
            self.off_task_since = None;
            self.off_task_boundary_fired = false;
            return None;
        }

        let since = *self.off_task_since.get_or_insert(timestamp);
        if !self.off_task_boundary_fired && timestamp.saturating_sub(since) >= OFF_TASK_DRIFT_THRESHOLD_SECS {
            self.off_task_boundary_fired = true;
            // The user has drifted away from whatever project they were in -
            // forget it so a return to that project is recognized as a fresh
            // switch rather than "no change".
            self.current_project = None;
            return Some(self.session_boundary(timestamp, "off_task_drift", "Drifted to unrelated browsing".to_string()));
        }
        None
    }

    /// Any non-browser activity, or on-task browsing, clears a pending
    /// off-task run - e.g. switching back to the editor before the drift
    /// threshold elapses shouldn't later trigger a stale boundary.
    pub fn note_on_task_activity(&mut self) {
        self.off_task_since = None;
        self.off_task_boundary_fired = false;
    }

    /// Stage 5 noise suppression: true if this is the same command + cwd as
    /// the last one, within `COMMAND_REPEAT_WINDOW_SECS` (up-arrow spam).
    pub fn is_repeated_command(&self, command: &str, cwd: &Option<String>, timestamp: u64) -> bool {
        match &self.last_command {
            Some((last_cmd, last_cwd, last_ts)) => {
                last_cmd == command && last_cwd == cwd && timestamp.saturating_sub(*last_ts) <= COMMAND_REPEAT_WINDOW_SECS
            }
            None => false,
        }
    }

    pub fn record_command(&mut self, command: &str, cwd: &Option<String>, timestamp: u64) {
        self.last_command = Some((command.to_string(), cwd.clone(), timestamp));
    }

    pub fn current_project(&self) -> Option<String> {
        self.current_project.clone()
    }

    pub fn record_test_failure(&mut self, timestamp: u64) {
        self.last_test_failure_ts = Some(timestamp);
    }

    /// Stage 4 correlation: if `timestamp` falls within
    /// `TEST_FAILURE_CORRELATION_WINDOW_SECS` of a recorded test failure,
    /// consume that flag and return true.
    pub fn check_test_failure_correlation(&mut self, timestamp: u64) -> bool {
        match self.last_test_failure_ts {
            Some(t) if timestamp >= t && timestamp - t <= TEST_FAILURE_CORRELATION_WINDOW_SECS => {
                self.last_test_failure_ts = None;
                true
            }
            _ => false,
        }
    }
}
