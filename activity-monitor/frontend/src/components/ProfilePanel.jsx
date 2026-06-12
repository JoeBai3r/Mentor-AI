import React from "react";

function formatDateTime(ts) {
  if (!ts) return "never";
  return new Date(ts * 1000).toLocaleString();
}

function MiniBar({value, color = "#4a90d9"}) {
  const pct = Math.max(0, Math.min(1, value || 0)) * 100;
  return (
    <div style={{background: "#eee", borderRadius: 3, height: 6, width: 60, display: "inline-block", verticalAlign: "middle"}}>
      <div style={{width: `${pct}%`, height: "100%", background: color, borderRadius: 3}} />
    </div>
  );
}

function Section({title, children}) {
  return (
    <div style={{marginBottom: 12}}>
      <div style={{fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#999", marginBottom: 4}}>
        {title}
      </div>
      {children}
    </div>
  );
}

function topEntries(obj, n = 5) {
  return Object.entries(obj || {}).sort((a, b) => b[1] - a[1]).slice(0, n);
}

export default function ProfilePanel({profile}) {
  if (!profile) {
    return <div style={{padding: 12, color: "#999", fontSize: 13}}>Loading profile…</div>;
  }

  const {identity, stack, rhythm, friction, behavioral, meta} = profile;

  const topPatterns = [...(friction?.friction_patterns || [])]
    .sort((a, b) => b.blocked_min_total - a.blocked_min_total)
    .slice(0, 4);

  return (
    <div style={{padding: 12, fontSize: 12}}>
      <Section title="Confidence">
        <div style={{display: "flex", justifyContent: "space-between", marginBottom: 2}}>
          <span>overall</span>
          <span><MiniBar value={meta?.confidence_overall} color="#27ae60" /> {meta?.confidence_overall?.toFixed(2)}</span>
        </div>
        {meta?.confidence_by_group && Object.entries(meta.confidence_by_group).map(([k, v]) => (
          <div key={k} style={{display: "flex", justifyContent: "space-between", color: "#777"}}>
            <span>{k}</span>
            <span><MiniBar value={v} /> {v.toFixed(2)}</span>
          </div>
        ))}
        <div style={{color: "#aaa", marginTop: 4}}>
          {meta?.sessions_total ?? 0} sessions distilled · last {formatDateTime(meta?.last_distilled_at)}
        </div>
      </Section>

      <Section title="Identity">
        <div>{identity?.role_self_reported}</div>
        <div style={{color: "#777"}}>{identity?.work_style_self_reported}</div>
      </Section>

      <Section title="Stack (top weights)">
        {topEntries(stack?.stack_weights).map(([name, weight]) => (
          <div key={name} style={{display: "flex", justifyContent: "space-between"}}>
            <span>{name}</span>
            <span><MiniBar value={weight} color="#4a90d9" /> {weight.toFixed(2)}</span>
          </div>
        ))}
        {topEntries(stack?.stack_weights).length === 0 && <div style={{color: "#aaa"}}>no data yet</div>}
      </Section>

      <Section title="Rhythm">
        <div>avg session: {rhythm?.avg_session_len_min?.toFixed(1)} min (±{rhythm?.session_len_stddev?.toFixed(1)})</div>
        <div>focus style: {rhythm?.focus_style} ({(rhythm?.focus_style_confidence * 100 || 0).toFixed(0)}% confidence)</div>
        <div>interruption gap: {rhythm?.interruption_gap_threshold_min} min</div>
        {rhythm?.typical_phase_durations && Object.entries(rhythm.typical_phase_durations).map(([phase, d]) => (
          <div key={phase} style={{color: "#777"}}>
            {phase}: {d.avg_min.toFixed(1)} min, resolves unassisted {(d.resolves_within_session * 100).toFixed(0)}%
          </div>
        ))}
      </Section>

      <Section title="Friction patterns">
        {topPatterns.length === 0 && <div style={{color: "#aaa"}}>no data yet</div>}
        {topPatterns.map(p => (
          <div key={p.id} style={{marginBottom: 4}}>
            <div style={{display: "flex", justifyContent: "space-between"}}>
              <span>
                {p.tool} · {p.error_class}
                <span style={{
                  marginLeft: 6, fontSize: 10, padding: "1px 5px", borderRadius: 3,
                  background: p.status === "confirmed" ? "#16a085" : "#bdc3c7",
                  color: "#fff",
                }}>
                  {p.status}
                </span>
              </span>
              <span style={{color: "#c0392b"}}>{p.blocked_min_total.toFixed(1)} min</span>
            </div>
            <div style={{color: "#aaa"}}>{p.count}x · last seen {formatDateTime(p.last_seen)}</div>
          </div>
        ))}
      </Section>

      <Section title="Behavioral">
        <div>interruption tolerance: <MiniBar value={behavioral?.interruption_tolerance} /> {behavioral?.interruption_tolerance?.toFixed(2)}</div>
        <div>search-before-try: <MiniBar value={behavioral?.search_before_try_ratio} /> {behavioral?.search_before_try_ratio?.toFixed(2)}</div>
        <div>context switches/day: {behavioral?.context_switch_freq_per_day?.toFixed(1)}</div>
        <div style={{color: "#777"}}>prefers {behavioral?.pref_recommendation_verbosity} suggestions, {behavioral?.pref_recommendation_timing}</div>
      </Section>
    </div>
  );
}
