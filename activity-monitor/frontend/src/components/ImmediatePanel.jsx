import React from "react";

const SIGNAL_COLORS = {
  error_loop: "#c0392b",
  context_switch: "#e67e22",
  repetition: "#8e44ad",
  idle: "#7f8c8d",
  none: "#27ae60",
};

const SIGNAL_LABELS = {
  error_loop: "Error loop",
  context_switch: "Context switch",
  repetition: "Repetition",
  idle: "Idle",
  none: "Nominal",
};

function scoreColor(score) {
  if (score >= 0.66) return "#c0392b";
  if (score >= 0.33) return "#e67e22";
  return "#27ae60";
}

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString();
}

function BlockingScoreBar({score, delta}) {
  const pct = Math.max(0, Math.min(1, score || 0)) * 100;
  return (
    <div>
      <div style={{display: "flex", justifyContent: "space-between", fontSize: 12, marginBottom: 2}}>
        <span>Blocking score</span>
        <span style={{fontFamily: "monospace"}}>
          {score?.toFixed(2) ?? "0.00"}
          {delta !== undefined && delta !== 0 && (
            <span style={{color: delta > 0 ? "#c0392b" : "#27ae60", marginLeft: 6}}>
              {delta > 0 ? "▲" : "▼"} {Math.abs(delta).toFixed(2)}
            </span>
          )}
        </span>
      </div>
      <div style={{background: "#eee", borderRadius: 4, height: 10, overflow: "hidden"}}>
        <div style={{width: `${pct}%`, height: "100%", background: scoreColor(score || 0), transition: "width 0.4s ease"}} />
      </div>
    </div>
  );
}

function GateBadge({gate, gateOpenAt}) {
  if (!gate) return null;
  const isOpen = gate.startsWith("open");
  const reason = gate.split(":")[1] || gate;
  return (
    <span style={{
      display: "inline-block",
      padding: "2px 8px",
      borderRadius: 4,
      fontSize: 11,
      fontWeight: 600,
      color: "#fff",
      background: isOpen ? "#16a085" : "#95a5a6",
    }}>
      {isOpen ? "OPEN" : "CLOSED"} · {reason}
      {isOpen && gateOpenAt && <span style={{fontWeight: 400}}> since {formatTime(gateOpenAt)}</span>}
    </span>
  );
}

function ErrorContextDetails({ctx}) {
  if (!ctx) return null;
  return (
    <ul style={{margin: "6px 0 0", paddingLeft: 18, fontSize: 12, color: "#444"}}>
      <li>tool: <code>{ctx.tool}{ctx.subcommand ? ` ${ctx.subcommand}` : ""}</code></li>
      <li>{ctx.failure_count} failures, {ctx.search_count} searches over {ctx.duration_sec}s</li>
      {ctx.domain && <li>searched on <code>{ctx.domain}</code></li>}
    </ul>
  );
}

function RepetitionDetails({ctx}) {
  if (!ctx) return null;
  return (
    <ul style={{margin: "6px 0 0", paddingLeft: 18, fontSize: 12, color: "#444"}}>
      <li>repeated {ctx.count}x: <code>{ctx.sequence?.join(" → ")}</code></li>
    </ul>
  );
}

function ContextSwitchDetails({ctx}) {
  if (!ctx) return null;
  return (
    <ul style={{margin: "6px 0 0", paddingLeft: 18, fontSize: 12, color: "#444"}}>
      <li>{ctx.from_project ?? "?"} → {ctx.to_project ?? "?"}{ctx.mid_task ? " (mid-task)" : ""}</li>
    </ul>
  );
}

export default function ImmediatePanel({state}) {
  if (!state) {
    return (
      <div style={{padding: 12, color: "#999", fontSize: 13}}>
        Waiting for activity…
      </div>
    );
  }

  const {signal = "none", blocking_score, blocking_score_delta, gate, gate_open_at, ts,
         error_context, repetition_context, context_switch} = state;

  return (
    <div style={{padding: 12}}>
      <div style={{display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10}}>
        <span style={{
          display: "inline-block", padding: "3px 10px", borderRadius: 4,
          fontSize: 12, fontWeight: 700, color: "#fff",
          background: SIGNAL_COLORS[signal] || "#7f8c8d",
        }}>
          {SIGNAL_LABELS[signal] || signal}
        </span>
        <span style={{fontSize: 11, color: "#999", fontFamily: "monospace"}}>{formatTime(ts)}</span>
      </div>

      <BlockingScoreBar score={blocking_score} delta={blocking_score_delta} />

      <div style={{marginTop: 10}}>
        <GateBadge gate={gate} gateOpenAt={gate_open_at} />
      </div>

      {signal === "error_loop" && <ErrorContextDetails ctx={error_context} />}
      {signal === "repetition" && <RepetitionDetails ctx={repetition_context} />}
      {signal === "context_switch" && <ContextSwitchDetails ctx={context_switch} />}
    </div>
  );
}
