//! Stage 2 (aggregation) and Stage 3 (decay + promotion) of the distiller:
//! folds a [`SessionIntent`] into the long-term [`UserProfile`] using the
//! aggregation strategy documented on each profile field.

use super::SessionIntent;
use crate::profile::{CommandSequence, ConfidenceByGroup, DocDomain, FrictionPattern, PhaseDuration, Project, ToolProficiency, UserProfile};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Multiplicative decay applied to recency-weighted maps every session.
const DECAY: f64 = 0.95;
/// How much a touched stack/tool weight is bumped per session.
const STACK_INCREMENT: f64 = 0.15;
/// How much a doc-domain weight is bumped per visit-bearing session.
const DOC_DOMAIN_INCREMENT: f64 = 0.2;
/// Smoothing factor for exponential moving averages.
pub(crate) const EMA_ALPHA: f64 = 0.3;
/// Sessions a friction pattern must appear in before `status` -> `"confirmed"`.
const FRICTION_PROMOTE_THRESHOLD: u32 = 3;
/// Sessions a command sequence must appear in before `status` -> `"confirmed"`.
const COMMAND_SEQ_PROMOTE_THRESHOLD: u32 = 5;
/// Entries decayed below this weight are dropped (unless promoted/confirmed).
const MIN_WEIGHT: f64 = 0.02;
const MAX_DOC_DOMAINS: usize = 10;
const MAX_ACTIVE_BRANCHES: usize = 5;
const MAX_COMMAND_SEQUENCES: usize = 20;

/// Folds `session` into `profile`, using `recent` (the previous session
/// intents, most-recent-first, capped at 19) for rolling-window
/// computations.
pub fn distill(profile: &mut UserProfile, session: &SessionIntent, recent: &[SessionIntent]) {
    update_stack(profile, session);
    update_projects(profile, session);
    update_rhythm(profile, session, recent);
    update_friction(profile, session);
    update_behavioral(profile, session);
    update_meta(profile, session);
}

pub(crate) fn ema(old: f64, sample: f64, alpha: f64) -> f64 {
    old * (1.0 - alpha) + sample * alpha
}

fn decay_and_prune(map: &mut HashMap<String, f64>, factor: f64) {
    for v in map.values_mut() {
        *v *= factor;
    }
    map.retain(|_, v| *v >= MIN_WEIGHT);
}

fn bump(map: &mut HashMap<String, f64>, key: &str, inc: f64) {
    let entry = map.entry(key.to_string()).or_insert(0.0);
    *entry = (*entry + inc).min(1.0);
}

/// Group 2 (Stack + tooling) - frequency aggregator.
fn update_stack(profile: &mut UserProfile, session: &SessionIntent) {
    let stack = &mut profile.stack;

    decay_and_prune(&mut stack.stack_weights, DECAY);
    for tp in stack.tool_proficiency.values_mut() {
        tp.weight *= DECAY;
    }
    for d in stack.doc_domains.iter_mut() {
        d.weight *= DECAY;
    }

    for app in session.app_focus_counts.keys() {
        bump(&mut stack.stack_weights, app, STACK_INCREMENT);
    }

    for tool in session.tool_counts.keys() {
        bump(&mut stack.stack_weights, tool, STACK_INCREMENT);

        let entry = stack
            .tool_proficiency
            .entry(tool.clone())
            .or_insert_with(|| ToolProficiency { weight: 0.0, avg_blocked_min_per_session: 0.0 });
        entry.weight = (entry.weight + STACK_INCREMENT).min(1.0);

        // A session where this tool's tests failed counts as "blocked time"
        // for that tool; sessions without failures pull the average down.
        let blocked_sample = if session.test_failure_tools.contains_key(tool) { session.duration_min } else { 0.0 };
        entry.avg_blocked_min_per_session = ema(entry.avg_blocked_min_per_session, blocked_sample, EMA_ALPHA);
    }
    stack.tool_proficiency.retain(|_, tp| tp.weight >= MIN_WEIGHT);

    for domain in session.doc_domain_counts.keys() {
        match stack.doc_domains.iter_mut().find(|d| &d.domain == domain) {
            Some(existing) => existing.weight = (existing.weight + DOC_DOMAIN_INCREMENT).min(1.0),
            None => stack.doc_domains.push(DocDomain { domain: domain.clone(), weight: DOC_DOMAIN_INCREMENT }),
        }
    }
    stack.doc_domains.retain(|d| d.weight >= MIN_WEIGHT);
    stack.doc_domains.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(Ordering::Equal));
    stack.doc_domains.truncate(MAX_DOC_DOMAINS);
}

/// Group 4 (Projects) - frequency aggregator.
///
/// Note: normalized events carry only the project's directory *name* (see
/// `entities.project` in `crate::normalize`), not its absolute path, so
/// `Project::root` is left empty for newly created entries until entity
/// extraction is extended to include the resolved project root.
fn update_projects(profile: &mut UserProfile, session: &SessionIntent) {
    let now = session.end_ts;
    let phase_total: f64 = session.phase_counts.values().sum::<u64>() as f64;

    for name in session.project_event_counts.keys() {
        let project = match profile.projects.projects.iter().position(|p| &p.name == name) {
            Some(i) => &mut profile.projects.projects[i],
            None => {
                profile.projects.projects.push(Project {
                    id: name.clone(),
                    name: name.clone(),
                    root: String::new(),
                    primary_stack: vec![],
                    last_active: 0,
                    session_count: 0,
                    phase_distribution: HashMap::new(),
                    active_branches: vec![],
                    collaborators: vec![],
                });
                profile.projects.projects.last_mut().unwrap()
            }
        };

        project.last_active = now;
        project.session_count += 1;

        if phase_total > 0.0 {
            let touched: HashSet<&String> = session.phase_counts.keys().collect();
            for (phase, count) in &session.phase_counts {
                let fraction = *count as f64 / phase_total;
                let entry = project.phase_distribution.entry(phase.clone()).or_insert(0.0);
                *entry = ema(*entry, fraction, EMA_ALPHA);
            }
            // Phases not touched this session decay toward 0 so the
            // distribution stays roughly normalized over time.
            for (phase, value) in project.phase_distribution.iter_mut() {
                if !touched.contains(phase) {
                    *value = ema(*value, 0.0, EMA_ALPHA);
                }
            }
        }

        if let Some(branches) = session.project_branches.get(name) {
            for branch in branches {
                project.active_branches.retain(|b| b != branch);
                project.active_branches.push(branch.clone());
            }
            if project.active_branches.len() > MAX_ACTIVE_BRANCHES {
                let excess = project.active_branches.len() - MAX_ACTIVE_BRANCHES;
                project.active_branches.drain(0..excess);
            }
        }
    }

    if let Some(primary) = &session.primary_project {
        profile.projects.last_active_project = Some(primary.clone());
    }

    // cross_project_patterns: TODO - promote from `friction.command_sequences`
    // once a confirmed sequence's `projects` list spans more than one entry.
}

/// Group 3 (Work rhythm) - rolling average + pattern accumulator.
fn update_rhythm(profile: &mut UserProfile, session: &SessionIntent, recent: &[SessionIntent]) {
    let rhythm = &mut profile.rhythm;

    // Rolling average/stddev over the last <=20 sessions (this one + up to
    // 19 prior, `recent` is most-recent-first).
    let mut durations: Vec<f64> = vec![session.duration_min];
    durations.extend(recent.iter().map(|s| s.duration_min));
    let n = durations.len() as f64;
    let mean = durations.iter().sum::<f64>() / n;
    let variance = durations.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n;
    rhythm.avg_session_len_min = mean;
    rhythm.session_len_stddev = variance.sqrt();

    // active_hours: EMA toward 1 for hours touched this session, toward 0
    // for hours that weren't - so the distribution gradually reflects
    // actual usage regardless of the seeded 9-5 prior.
    for hour in 0u8..24 {
        let key = format!("{:02}", hour);
        let touched = session.active_hours.contains(&hour);
        let entry = rhythm.active_hours.entry(key).or_insert(0.0);
        *entry = ema(*entry, if touched { 1.0 } else { 0.0 }, EMA_ALPHA);
    }

    // typical_phase_durations: EMA the minutes spent in each phase.
    let phase_total: f64 = session.phase_counts.values().sum::<u64>() as f64;
    if phase_total > 0.0 {
        for (phase, count) in &session.phase_counts {
            let minutes = session.duration_min * (*count as f64 / phase_total);
            let entry = rhythm
                .typical_phase_durations
                .entry(phase.clone())
                .or_insert(PhaseDuration { avg_min: minutes, resolves_within_session: 0.6 });
            entry.avg_min = ema(entry.avg_min, minutes, EMA_ALPHA);
        }
        if session.dominant_phase == "debugging" {
            if let Some(debugging) = rhythm.typical_phase_durations.get_mut("debugging") {
                // No test failures at all -> trivially resolved. Otherwise,
                // defer to the immediate intent layer's blocking-score
                // trend if it observed anything: a session that ends with
                // the score having eased back down (and not still high)
                // counts as resolved even though tests failed at some
                // point; one that ends flat/rising while still elevated
                // counts as unresolved.
                let resolved = if session.test_failures == 0 {
                    1.0
                } else if session.max_blocking_score > 0.0 {
                    if session.blocking_score_trend < 0.0 && session.final_blocking_score < 0.3 { 1.0 } else { 0.0 }
                } else {
                    0.0
                };
                debugging.resolves_within_session = ema(debugging.resolves_within_session, resolved, EMA_ALPHA);
            }
        }
    }

    // phase_sequence: top recurring consecutive-phase transitions over the
    // same rolling window, oldest -> newest.
    let mut phases: Vec<String> = recent.iter().rev().map(|s| s.dominant_phase.clone()).collect();
    phases.push(session.dominant_phase.clone());
    let mut pair_counts: HashMap<(String, String), u32> = HashMap::new();
    for w in phases.windows(2) {
        *pair_counts.entry((w[0].clone(), w[1].clone())).or_insert(0) += 1;
    }
    let mut pairs: Vec<((String, String), u32)> = pair_counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1));
    if !pairs.is_empty() {
        rhythm.phase_sequence = pairs.into_iter().take(3).map(|((a, b), _)| vec![a, b]).collect();
    }

    // interruption_gap_threshold_min: TODO - recalibrating this needs the
    // distribution of inactivity gaps between activity bursts, which isn't
    // tracked at the session-intent level yet (only whether a gap *caused* a
    // session boundary). Left at its current value.
}

/// Group 5 (Friction patterns) - pattern accumulator.
fn update_friction(profile: &mut UserProfile, session: &SessionIntent) {
    // Cloned up front so `friction` can be borrowed mutably below without
    // conflicting with `profile.identity`.
    let self_reported = profile.identity.friction_self_reported.clone();
    let friction = &mut profile.friction;

    for fp in friction.friction_patterns.iter_mut() {
        fp.weight *= DECAY;
    }
    for cs in friction.command_sequences.iter_mut() {
        cs.weight *= DECAY;
    }

    for tool in session.test_failure_tools.keys() {
        // Prefer the immediate intent layer's directly-observed error-loop
        // duration for this tool over the phase-fraction estimate - it's
        // measured from the actual failure/search/retry span rather than
        // inferred from how normalized events were classified.
        let blocked_min = match session.error_loops.iter().find(|e| &e.tool == tool) {
            Some(loop_summary) => loop_summary.duration_sec as f64 / 60.0,
            None => {
                let total: f64 = session.phase_counts.values().sum::<u64>() as f64;
                let debugging_fraction = if total > 0.0 { *session.phase_counts.get("debugging").unwrap_or(&0) as f64 / total } else { 0.0 };
                session.duration_min * debugging_fraction
            }
        };

        let pattern = match friction.friction_patterns.iter().position(|p| &p.tool == tool && p.error_class == "test_failure") {
            Some(i) => &mut friction.friction_patterns[i],
            None => {
                friction.friction_patterns.push(FrictionPattern {
                    id: format!("fp_{}_test_failure", tool),
                    tool: tool.clone(),
                    error_class: "test_failure".to_string(),
                    status: "candidate".to_string(),
                    count: 0,
                    blocked_min_total: 0.0,
                    first_seen: session.start_ts,
                    last_seen: session.start_ts,
                    weight: 0.0,
                    resolution_paths: vec![],
                    self_reported_match: false,
                });
                friction.friction_patterns.last_mut().unwrap()
            }
        };

        pattern.count += 1;
        pattern.blocked_min_total += blocked_min;
        pattern.last_seen = session.end_ts;
        pattern.weight = (pattern.count as f64 / 10.0).min(1.0);
        if pattern.count >= FRICTION_PROMOTE_THRESHOLD {
            pattern.status = "confirmed".to_string();
        }

        let matches_self_report = self_reported.iter().any(|f| {
            let lower = f.to_lowercase();
            lower.contains(&tool.to_lowercase()) || lower.contains("test") || lower.contains("debug")
        });
        if matches_self_report {
            pattern.self_reported_match = true;
            if !friction.friction_confirmed.contains(tool) {
                friction.friction_confirmed.push(tool.clone());
            }
        }
    }

    // command_sequences: consecutive "tool subcommand" bigrams, counted at
    // most once per session so a loop within a session doesn't inflate the
    // count.
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for w in session.command_sequence.windows(2) {
        let pair = (w[0].clone(), w[1].clone());
        if !seen.insert(pair.clone()) {
            continue;
        }
        let (a, b) = pair;
        let cs = match friction.command_sequences.iter().position(|c| c.seq.len() == 2 && c.seq[0] == a && c.seq[1] == b) {
            Some(i) => &mut friction.command_sequences[i],
            None => {
                friction.command_sequences.push(CommandSequence {
                    seq: vec![a, b],
                    count: 0,
                    last_seen: 0,
                    weight: 0.0,
                    status: "candidate".to_string(),
                    projects: vec![],
                });
                friction.command_sequences.last_mut().unwrap()
            }
        };
        cs.count += 1;
        cs.last_seen = session.end_ts;
        cs.weight = (cs.count as f64 / 10.0).min(1.0);
        if cs.count >= COMMAND_SEQ_PROMOTE_THRESHOLD {
            cs.status = "confirmed".to_string();
        }
        if let Some(project) = &session.primary_project {
            if !cs.projects.contains(project) {
                cs.projects.push(project.clone());
            }
        }
    }

    friction.friction_patterns.retain(|p| p.weight >= MIN_WEIGHT || p.status == "confirmed");
    friction.command_sequences.retain(|c| c.weight >= MIN_WEIGHT || c.status == "confirmed");
    friction.command_sequences.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(Ordering::Equal));
    friction.command_sequences.truncate(MAX_COMMAND_SEQUENCES);

    // resolution_paths.error_loop: how test failures got resolved this
    // session, EMA-blended into the running distribution.
    if !session.friction_resolutions.is_empty() {
        let mut counts: HashMap<&str, f64> = HashMap::new();
        for r in &session.friction_resolutions {
            *counts.entry(r.as_str()).or_insert(0.0) += 1.0;
        }
        let total = session.friction_resolutions.len() as f64;
        let entry = friction.resolution_paths.entry("error_loop".to_string()).or_default();
        for key in ["doc_lookup", "trial_error", "step_away"] {
            let sample = counts.get(key).copied().unwrap_or(0.0) / total;
            let old = *entry.get(key).unwrap_or(&0.0);
            entry.insert(key.to_string(), ema(old, sample, EMA_ALPHA));
        }
    }
    // resolution_paths.stuck_general: TODO - needs a signal for non-test
    // "stuck" states (e.g. long pauses without commits/file changes), which
    // the session summarizer doesn't classify yet. Left at its seeded value.
}

/// Group 6 (Behavioral tendencies) - rolling average for the one signal we
/// can currently derive.
fn update_behavioral(profile: &mut UserProfile, session: &SessionIntent) {
    let behavioral = &mut profile.behavioral;

    // search_before_try_ratio: only updated when the session contains both a
    // research-style navigation and a failed command, so we know which came
    // first.
    if let Some(searched_first) = session.search_before_try {
        let sample = if searched_first { 1.0 } else { 0.0 };
        behavioral.search_before_try_ratio = ema(behavioral.search_before_try_ratio, sample, EMA_ALPHA);
    }

    // interruption_tolerance, context_switch_freq_per_day,
    // recommendation_accept_rate, pref_recommendation_*: TODO. These need
    // signals (interruption/notification responses, calendar-aware daily
    // session counts, recommendation outcomes) that don't exist yet.
}

/// Group 7 (Profile meta + health) - decay + promotion bookkeeping.
fn update_meta(profile: &mut UserProfile, session: &SessionIntent) {
    let now = session.end_ts;
    let confirmed_friction = profile.friction.friction_patterns.iter().filter(|p| p.status == "confirmed").count();

    let meta = &mut profile.meta;
    meta.sessions_total += 1;
    meta.sessions_in_window = meta.sessions_total.min(20);
    meta.last_distilled_at = Some(now);
    meta.decay_last_run = Some(now);

    let sessions_total = meta.sessions_total as f64;
    let stack_conf = (sessions_total / 20.0).min(1.0);
    let rhythm_conf = (meta.sessions_in_window as f64 / 20.0).min(1.0);
    let friction_conf = if confirmed_friction > 0 {
        (sessions_total / 30.0).min(1.0).max(0.6)
    } else {
        (sessions_total / 30.0).min(1.0)
    };
    let behavioral_conf = (sessions_total / 15.0).min(1.0);

    meta.confidence_by_group = ConfidenceByGroup {
        stack: stack_conf,
        rhythm: rhythm_conf,
        friction: friction_conf,
        behavioral: behavioral_conf,
    };
    meta.confidence_overall = (stack_conf + rhythm_conf + friction_conf + behavioral_conf) / 4.0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::UserProfile;

    fn sample_session() -> SessionIntent {
        let mut tool_counts = HashMap::new();
        tool_counts.insert("cargo".to_string(), 3);

        let mut test_failure_tools = HashMap::new();
        test_failure_tools.insert("cargo".to_string(), 1);

        let mut project_event_counts = HashMap::new();
        project_event_counts.insert("activity-monitor".to_string(), 10);

        let mut project_branches = HashMap::new();
        project_branches.insert("activity-monitor".to_string(), vec!["main".to_string()]);

        let mut app_focus_counts = HashMap::new();
        app_focus_counts.insert("vscode".to_string(), 5);

        let mut doc_domain_counts = HashMap::new();
        doc_domain_counts.insert("doc.rust-lang.org".to_string(), 2);

        let mut phase_counts = HashMap::new();
        phase_counts.insert("implementation".to_string(), 7);
        phase_counts.insert("debugging".to_string(), 3);

        SessionIntent {
            session_id: 1,
            start_ts: 1_000,
            end_ts: 1_000 + 60 * 30,
            duration_min: 30.0,
            end_reason: "inactivity_gap".to_string(),
            event_count: 20,
            project_event_counts,
            project_branches,
            primary_project: Some("activity-monitor".to_string()),
            app_focus_counts,
            active_hours: vec![10],
            commands_run: 3,
            commands_failed: 1,
            tool_counts,
            command_sequence: vec!["cargo build".to_string(), "cargo test".to_string()],
            files_changed: 4,
            browser_domain_counts: HashMap::new(),
            doc_domain_counts,
            browser_category_counts: HashMap::new(),
            search_queries: vec![],
            clipboard_events: 0,
            test_failures: 1,
            test_failure_tools,
            friction_resolutions: vec!["doc_lookup".to_string()],
            search_before_try: Some(true),
            phase_counts,
            dominant_phase: "implementation".to_string(),
            immediate_signal_counts: HashMap::new(),
            error_loops: vec![],
            max_blocking_score: 0.0,
            final_blocking_score: 0.0,
            blocking_score_trend: 0.0,
            gate_open_count: 0,
        }
    }

    #[test]
    fn distill_updates_profile_from_session() {
        let mut profile = UserProfile::seed_typical_developer();
        let session = sample_session();

        distill(&mut profile, &session, &[]);

        // Stack: touched app/tool weights bumped from zero.
        assert_eq!(profile.stack.stack_weights.get("vscode"), Some(&STACK_INCREMENT));
        assert_eq!(profile.stack.stack_weights.get("cargo"), Some(&STACK_INCREMENT));
        assert!(profile.stack.tool_proficiency.contains_key("cargo"));
        assert!(profile.stack.doc_domains.iter().any(|d| d.domain == "doc.rust-lang.org"));

        // Projects: new project created and updated.
        let project = profile.projects.projects.iter().find(|p| p.name == "activity-monitor").expect("project recorded");
        assert_eq!(project.session_count, 1);
        assert_eq!(project.last_active, session.end_ts);
        assert_eq!(project.active_branches, vec!["main".to_string()]);
        assert_eq!(profile.projects.last_active_project, Some("activity-monitor".to_string()));

        // Rhythm: rolling average reflects this session's duration.
        assert_eq!(profile.rhythm.avg_session_len_min, 30.0);
        assert!(profile.rhythm.active_hours.get("10").copied().unwrap_or(0.0) > 0.0);

        // Friction: candidate pattern created for cargo test failures.
        let pattern = profile.friction.friction_patterns.iter().find(|p| p.tool == "cargo" && p.error_class == "test_failure").expect("friction pattern recorded");
        assert_eq!(pattern.count, 1);
        assert_eq!(pattern.status, "candidate");
        assert!(profile.friction.command_sequences.iter().any(|c| c.seq == vec!["cargo build".to_string(), "cargo test".to_string()]));

        // Behavioral: search-before-try ratio nudged toward 1.
        assert!(profile.behavioral.search_before_try_ratio > 0.5);

        // Meta: session counted and confidence increased from zero.
        assert_eq!(profile.meta.sessions_total, 1);
        assert_eq!(profile.meta.sessions_in_window, 1);
        assert!(profile.meta.confidence_overall > 0.0);
    }

    #[test]
    fn friction_pattern_promotes_after_threshold() {
        let mut profile = UserProfile::seed_typical_developer();
        let session = sample_session();

        for _ in 0..FRICTION_PROMOTE_THRESHOLD {
            distill(&mut profile, &session, &[]);
        }

        let pattern = profile.friction.friction_patterns.iter().find(|p| p.tool == "cargo" && p.error_class == "test_failure").expect("friction pattern recorded");
        assert_eq!(pattern.count, FRICTION_PROMOTE_THRESHOLD);
        assert_eq!(pattern.status, "confirmed");
    }
}
