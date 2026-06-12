import React from "react";

const PHASE_COLORS = {
  implementation: "#2e7d32",
  debugging: "#c0392b",
  research: "#2980b9",
  exploration: "#8e44ad",
};

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString();
}

function scoreColor(score) {
  if (score >= 0.66) return "#c0392b";
  if (score >= 0.33) return "#e67e22";
  return "#27ae60";
}

function SessionRow({session, isLive}) {
  const {
    session_id, start_ts, end_ts, duration_min, end_reason, primary_project,
    dominant_phase, commands_run, commands_failed, test_failures,
    max_blocking_score, error_loops, gate_open_count,
  } = session;

  return (
    <li style={{padding: "8px 12px", borderBottom: "1px solid #eee", fontSize: 12}}>
      <div style={{display: "flex", justifyContent: "space-between", alignItems: "baseline"}}>
        <span style={{fontWeight: 600}}>
          Session #{session_id}{isLive && <span style={{color: "#27ae60", marginLeft: 6}}>● live</span>}
        </span>
        <span style={{color: "#999", fontFamily: "monospace", fontSize: 11}}>
          {formatTime(start_ts)} – {formatTime(end_ts)} · {duration_min?.toFixed(1)}m
        </span>
      </div>
      <div style={{marginTop: 4, display: "flex", flexWrap: "wrap", gap: 6, alignItems: "center"}}>
        {primary_project && (
          <span style={{background: "#eef", border: "1px solid #ccd", borderRadius: 4, padding: "1px 6px"}}>
            {primary_project}
          </span>
        )}
        {dominant_phase && (
          <span style={{
            background: PHASE_COLORS[dominant_phase] || "#7f8c8d", color: "#fff",
            borderRadius: 4, padding: "1px 6px",
          }}>
            {dominant_phase}
          </span>
        )}
        <span style={{color: "#555"}}>{commands_run ?? 0} cmds, {commands_failed ?? 0} failed</span>
        {test_failures > 0 && <span style={{color: "#c0392b"}}>{test_failures} test failures</span>}
        {max_blocking_score > 0 && (
          <span style={{color: scoreColor(max_blocking_score), fontWeight: 600}}>
            peak block {max_blocking_score.toFixed(2)}
          </span>
        )}
        {gate_open_count > 0 && (
          <span style={{color: "#16a085"}}>{gate_open_count} intervention{gate_open_count === 1 ? "" : "s"}</span>
        )}
        <span style={{color: "#aaa", marginLeft: "auto"}}>{end_reason}</span>
      </div>
      {error_loops?.length > 0 && (
        <div style={{marginTop: 4, color: "#c0392b"}}>
          {error_loops.map((e, i) => (
            <span key={i} style={{marginRight: 8}}>
              ⚠ {e.tool}{e.subcommand ? ` ${e.subcommand}` : ""} ({e.duration_sec}s, {e.failure_count}x)
            </span>
          ))}
        </div>
      )}
    </li>
  );
}

export default function SessionsPanel({sessions, liveSessionId}) {
  if (!sessions || sessions.length === 0) {
    return <div style={{padding: 12, color: "#999", fontSize: 13}}>No sessions yet…</div>;
  }
  return (
    <ul style={{listStyle: "none", padding: 0, margin: 0, maxHeight: 360, overflow: "auto"}}>
      {sessions.map(session => (
        <SessionRow key={session.session_id} session={session} isLive={session.session_id === liveSessionId} />
      ))}
    </ul>
  );
}
