//! The long-term user profile: the slowest-changing layer of the intent
//! system, persisted across sessions.
//!
//! This module only defines the schema and a generic "typical developer"
//! seed. The pipeline that updates it (session summarizer -> aggregators ->
//! decay/promotion) is a separate, later piece of work; for now the profile
//! is read-only data that a future distiller will overwrite wholesale via
//! `PUT /api/profile`.
//!
//! ## Producers and consumers, at a glance
//!
//! - **Onboarding questionnaire** (not yet built): the only writer of
//!   [`Identity`]. Until it exists, [`UserProfile::seed_typical_developer`]
//!   stands in as the prior.
//! - **Distiller** (not yet built): runs when a session closes. Each group
//!   below documents which aggregation strategy the distiller will use for
//!   it (recency-weighted frequency, rolling average over the last 20
//!   sessions, or pattern accumulation with promotion thresholds) and what
//!   it writes.
//! - **Recommendation engine**: the one *current* exception — it updates
//!   `behavioral.recommendation_accept_rate` directly when a suggestion is
//!   accepted or dismissed, since that's a direct outcome rather than
//!   something distilled from session history.
//! - **Session builder** / **intent inference** / **recommendation engine**:
//!   the three readers. Each group's doc comment notes which of these reads
//!   it and why.
//!
//! `meta.confidence_by_group` is the trust gate every reader should check
//! before leaning on a group's data: a freshly seeded profile has zero
//! confidence everywhere, even though every field has a plausible default
//! value to avoid "no data" special-casing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Bumped only when this schema changes (by a migration), never by the
/// distiller.
pub const PROFILE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserProfile {
    pub identity: Identity,
    pub stack: Stack,
    pub rhythm: Rhythm,
    pub projects: Projects,
    pub friction: Friction,
    pub behavioral: Behavioral,
    pub meta: Meta,
}

/// Group 1 - Identity + onboarding.
///
/// **Written by:** the onboarding questionnaire, once. The distiller never
/// overwrites these fields directly - `friction.friction_confirmed` is where
/// observation can confirm or contradict `friction_self_reported` without
/// touching the raw self-report.
///
/// **Read by:** intent inference, as the prior for role/work-style when
/// `meta.confidence_by_group` is still low; and the recommendation engine,
/// for which a *divergence* between `role_self_reported` and observed
/// activity (e.g. a "backend engineer" whose sessions are mostly CSS) is
/// itself a signal worth surfacing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub role_self_reported: String,
    pub friction_self_reported: Vec<String>,
    pub work_style_self_reported: String,
    pub onboarding_completed_at: u64,
    pub onboarding_version: u32,
}

/// Group 2 - Stack + tooling.
///
/// **Written by:** the distiller's frequency aggregator (recency-weighted
/// frequency count). Every session, the languages/tools/domains touched
/// (read off `entities.app`/`entities.tool`/`entities.domain` in normalized
/// events) get their weights bumped, and all weights decay slightly so stale
/// tools fade out.
///
/// **Read by:** intent inference, to judge whether the current
/// tool/domain/file type is familiar territory for this user (familiar ->
/// low-friction activity is unremarkable; unfamiliar -> the same activity
/// may indicate exploration or being stuck). Also read by the recommendation
/// engine: `tool_proficiency.avg_blocked_min_per_session` decides whether a
/// suggestion should be a terse fix or a conceptual explanation, and
/// `doc_domains` decides which docs to link first.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stack {
    pub stack_weights: HashMap<String, f64>,
    pub tool_proficiency: HashMap<String, ToolProficiency>,
    pub doc_domains: Vec<DocDomain>,
    pub shell: String,
    pub editor: String,
    pub os: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolProficiency {
    pub weight: f64,
    pub avg_blocked_min_per_session: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocDomain {
    pub domain: String,
    pub weight: f64,
}

/// Group 3 - Work rhythm.
///
/// **Written by:** the distiller's rolling-average aggregator (window of the
/// last 20 sessions) for `avg_session_len_min`, `session_len_stddev`,
/// `focus_style`/`focus_style_confidence`, and `active_hours`; and the
/// pattern accumulator for `phase_sequence` and `typical_phase_durations`,
/// which keep only the top 2-3 observed sequences/durations.
/// `interruption_gap_threshold_min` is recalibrated from the observed gap
/// distribution between activity bursts.
///
/// **Read by:** intent inference, heavily. `active_hours` combined with
/// `focus_style`/`focus_style_confidence` gates how readily a suggestion can
/// interrupt right now. `phase_sequence` + `typical_phase_durations` let the
/// session builder predict the likely next phase (e.g. "implementation ->
/// debugging" after a long implementation stretch, or flag that this user's
/// debugging sessions average 28 minutes and resolve unassisted 74% of the
/// time). `interruption_gap_threshold_min` is the personalized
/// session-boundary gap used by the normalizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rhythm {
    pub avg_session_len_min: f64,
    pub session_len_stddev: f64,
    /// `"deep"` or `"switching"`.
    pub focus_style: String,
    pub focus_style_confidence: f64,
    /// Hour-of-day ("00".."23") -> activity weight in [0, 1].
    pub active_hours: HashMap<String, f64>,
    /// Top 2-3 observed phase transition sequences, most common first.
    pub phase_sequence: Vec<Vec<String>>,
    pub typical_phase_durations: HashMap<String, PhaseDuration>,
    pub interruption_gap_threshold_min: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseDuration {
    pub avg_min: f64,
    pub resolves_within_session: f64,
}

/// Group 4 - Projects.
///
/// **Written by:** the distiller's frequency aggregator. Each session
/// updates `last_active`, `session_count`, `phase_distribution`, and
/// `active_branches`/`collaborators` for the project(s) touched (matched by
/// `root` against `entities.project`/`cwd` in normalized events); new
/// projects are appended. `cross_project_patterns` is promoted from
/// `friction.command_sequences` once a sequence is observed across more than
/// one project.
///
/// **Read by:** the session builder first - looking up `cwd` against
/// `projects[].root` immediately primes a new session with stack, phase
/// history, and active branches, which gives the goal classifier far better
/// context than cold inference. Intent inference also reads
/// `phase_distribution` as a per-project prior (a project that's
/// historically 80% debugging makes "stuck" the more likely read of
/// ambiguous activity there).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Projects {
    pub projects: Vec<Project>,
    pub last_active_project: Option<String>,
    /// Workflow patterns observed across more than one project - the
    /// strongest candidates for workflow automation since they're the
    /// user's general habits, not project-specific ones.
    pub cross_project_patterns: Vec<CrossProjectPattern>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub root: String,
    pub primary_stack: Vec<String>,
    pub last_active: u64,
    pub session_count: u32,
    pub phase_distribution: HashMap<String, f64>,
    pub active_branches: Vec<String>,
    pub collaborators: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossProjectPattern {
    pub pattern: String,
    pub projects: Vec<String>,
}

/// Group 5 - Friction patterns.
///
/// **Written by:** the distiller's pattern accumulator. Candidate patterns
/// (recurring errors, repeated command sequences) accumulate `count` and
/// `blocked_min_total`; once a pattern crosses a promotion threshold its
/// `status` moves from `"candidate"` to `"confirmed"`. When a confirmed
/// pattern's `tool`/`error_class` matches an entry in
/// `identity.friction_self_reported`, that entry is copied into
/// `friction_confirmed` and the pattern's `self_reported_match` is set.
/// `resolution_paths` is updated from what the user actually did right after
/// a friction event (next browser nav to docs, terminal trial-and-error, or
/// the session just ending).
///
/// **Read by:** the recommendation engine, primarily. `blocked_min_total`
/// ranks which friction is most worth automating; `status == "confirmed"`
/// together with `meta.confidence_by_group.friction` gates whether a
/// friction-remover suggestion fires at all; `resolution_paths` (split by
/// friction type) decides *what kind* of suggestion to make - e.g. a doc
/// link for `error_loop` if `doc_lookup` dominates there, versus a more
/// exploratory prompt for `stuck_general` if it's evenly split.
/// `command_sequences` feeds workflow-accelerator suggestions directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Friction {
    pub friction_patterns: Vec<FrictionPattern>,
    /// Self-reported friction items (from `identity.friction_self_reported`)
    /// that observation has confirmed.
    pub friction_confirmed: Vec<String>,
    pub command_sequences: Vec<CommandSequence>,
    /// Friction type (e.g. `"error_loop"`, `"stuck_general"`) -> resolution
    /// strategy -> share of occurrences resolved that way.
    pub resolution_paths: HashMap<String, HashMap<String, f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrictionPattern {
    pub id: String,
    pub tool: String,
    pub error_class: String,
    /// `"candidate"` or `"confirmed"`.
    pub status: String,
    pub count: u32,
    pub blocked_min_total: f64,
    pub first_seen: u64,
    pub last_seen: u64,
    pub weight: f64,
    pub resolution_paths: Vec<String>,
    pub self_reported_match: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandSequence {
    pub seq: Vec<String>,
    pub count: u32,
    pub last_seen: u64,
    pub weight: f64,
    /// `"candidate"` or `"confirmed"`.
    pub status: String,
    pub projects: Vec<String>,
}

/// Group 6 - Behavioral tendencies.
///
/// **Written by:** the distiller's rolling-average aggregator for
/// `interruption_tolerance`, `search_before_try_ratio`, and
/// `context_switch_freq_per_day`. The recommendation engine is the one
/// direct (non-distiller) writer in the whole profile: it updates
/// `recommendation_accept_rate` immediately when a suggestion is accepted or
/// dismissed, since that's an outcome rather than something distilled from
/// session history.
///
/// **Read by:** the recommendation engine - `recommendation_accept_rate` per
/// suggestion type re-weights which types fire at all;
/// `search_before_try_ratio` chooses explanatory vs. terse phrasing.
/// `pref_recommendation_verbosity`/`pref_recommendation_timing` start as
/// defaults derived from `rhythm.focus_style`, but once a field's name
/// appears in `pref_locked` (the user set it explicitly) it becomes a hard
/// constraint that distillation must not overwrite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Behavioral {
    pub interruption_tolerance: f64,
    /// Suggestion type -> historical accept rate.
    pub recommendation_accept_rate: HashMap<String, f64>,
    pub search_before_try_ratio: f64,
    pub context_switch_freq_per_day: f64,
    /// `"concise"` or `"detailed"`.
    pub pref_recommendation_verbosity: String,
    /// e.g. `"on_pause"`.
    pub pref_recommendation_timing: String,
    /// Names of `pref_*` fields the user has explicitly set (via settings or
    /// the questionnaire) - these are never overwritten by inferred
    /// defaults.
    pub pref_locked: Vec<String>,
}

/// Group 7 - Profile meta + health.
///
/// **Written by:** the distiller's decay + promotion stage, every time it
/// runs (after each session closes). Bumps `sessions_total`/
/// `sessions_in_window`, recomputes `confidence_overall` and
/// `confidence_by_group` from how much and how recent the data behind each
/// group is, and records `last_distilled_at`/`decay_last_run`.
/// `profile_schema_version` is bumped only by schema migrations.
///
/// **Read by:** every consumer, as the trust gate checked before leaning on
/// a group's data - e.g. intent inference falls back to generic priors while
/// `confidence_by_group.rhythm` is low, and the recommendation engine
/// requires `confidence_by_group.friction` above a threshold before treating
/// `friction.friction_patterns` as reliable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub created_at: u64,
    pub last_distilled_at: Option<u64>,
    pub sessions_total: u32,
    pub sessions_in_window: u32,
    pub confidence_overall: f64,
    pub confidence_by_group: ConfidenceByGroup,
    pub decay_last_run: Option<u64>,
    pub profile_schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceByGroup {
    pub stack: f64,
    pub rhythm: f64,
    pub friction: f64,
    pub behavioral: f64,
}

impl UserProfile {
    /// A generic "typical developer" baseline, used until the onboarding
    /// questionnaire exists. All `meta.confidence_*` fields start at 0 -
    /// nothing has been observed yet - but every other field gets a
    /// plausible prior so downstream consumers never need to special-case
    /// "no data".
    pub fn seed_typical_developer() -> Self {
        let now = chrono::Utc::now().timestamp() as u64;

        let shell = std::env::var("SHELL")
            .ok()
            .and_then(|s| Path::new(&s).file_name().map(|n| n.to_string_lossy().to_string()))
            .unwrap_or_else(|| "bash".to_string());
        let os = std::env::consts::OS.to_string();

        UserProfile {
            identity: Identity {
                role_self_reported: "software developer".to_string(),
                friction_self_reported: vec![],
                work_style_self_reported: "mixed: deep focus with periodic context switches".to_string(),
                onboarding_completed_at: now,
                onboarding_version: 1,
            },
            stack: Stack {
                stack_weights: HashMap::new(),
                tool_proficiency: HashMap::new(),
                doc_domains: vec![],
                shell,
                editor: "vscode".to_string(),
                os,
            },
            rhythm: Rhythm {
                avg_session_len_min: 45.0,
                session_len_stddev: 20.0,
                focus_style: "mixed".to_string(),
                focus_style_confidence: 0.0,
                active_hours: default_active_hours(),
                phase_sequence: vec![
                    vec!["implementation".to_string(), "debugging".to_string()],
                    vec!["exploration".to_string(), "implementation".to_string()],
                ],
                typical_phase_durations: {
                    let mut m = HashMap::new();
                    m.insert("debugging".to_string(), PhaseDuration { avg_min: 25.0, resolves_within_session: 0.6 });
                    m
                },
                interruption_gap_threshold_min: 5,
            },
            projects: Projects {
                projects: vec![],
                last_active_project: None,
                cross_project_patterns: vec![],
            },
            friction: Friction {
                friction_patterns: vec![],
                friction_confirmed: vec![],
                command_sequences: vec![],
                resolution_paths: default_resolution_paths(),
            },
            behavioral: Behavioral {
                interruption_tolerance: 0.5,
                recommendation_accept_rate: default_accept_rates(),
                search_before_try_ratio: 0.5,
                context_switch_freq_per_day: 4.0,
                pref_recommendation_verbosity: "concise".to_string(),
                pref_recommendation_timing: "on_pause".to_string(),
                pref_locked: vec![],
            },
            meta: Meta {
                created_at: now,
                last_distilled_at: None,
                sessions_total: 0,
                sessions_in_window: 0,
                confidence_overall: 0.0,
                confidence_by_group: ConfidenceByGroup { stack: 0.0, rhythm: 0.0, friction: 0.0, behavioral: 0.0 },
                decay_last_run: None,
                profile_schema_version: PROFILE_SCHEMA_VERSION,
            },
        }
    }
}

/// Generic 9-to-5 weekday curve, ramping up mid-morning and mid-afternoon
/// with a lunch dip - a neutral starting point until `active_hours` is
/// distilled from real session timestamps.
fn default_active_hours() -> HashMap<String, f64> {
    [("09", 0.4), ("10", 0.7), ("11", 0.8), ("12", 0.4), ("13", 0.3), ("14", 0.7), ("15", 0.8), ("16", 0.6), ("17", 0.3)]
        .iter()
        .map(|(h, w)| (h.to_string(), *w))
        .collect()
}

/// Neutral prior: getting unstuck mostly via docs, then trial-and-error,
/// rarely by stepping away - applied to both friction types until observed
/// resolutions diverge.
fn default_resolution_paths() -> HashMap<String, HashMap<String, f64>> {
    fn neutral() -> HashMap<String, f64> {
        [("doc_lookup", 0.5), ("trial_error", 0.35), ("step_away", 0.15)]
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect()
    }
    let mut m = HashMap::new();
    m.insert("error_loop".to_string(), neutral());
    m.insert("stuck_general".to_string(), neutral());
    m
}

/// Neutral 50% prior for every recommendation type until acceptance/decline
/// outcomes are recorded.
fn default_accept_rates() -> HashMap<String, f64> {
    ["friction_remover", "knowledge_bridge", "workflow_accelerator", "state_preserver"]
        .iter()
        .map(|k| (k.to_string(), 0.5))
        .collect()
}
