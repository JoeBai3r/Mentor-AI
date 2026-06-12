import React, {useState} from "react";

const REC_TYPE_LABELS = {
  friction_remover: "Friction remover",
  knowledge_bridge: "Knowledge bridge",
  workflow_accelerator: "Workflow accelerator",
  state_preserver: "State preserver",
};

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString();
}

function RecommendationCard({rec, onFeedback}) {
  const [busy, setBusy] = useState(false);
  const decided = rec.status !== "pending";

  const send = async (accepted) => {
    setBusy(true);
    try {
      await onFeedback(rec.id, accepted);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{padding: 12, borderBottom: "1px solid #eee"}}>
      <div style={{display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 6}}>
        <span style={{fontSize: 11, fontWeight: 600, color: "#16a085", textTransform: "uppercase"}}>
          {REC_TYPE_LABELS[rec.rec_type] || rec.rec_type}
        </span>
        <span style={{fontSize: 11, color: "#999", fontFamily: "monospace"}}>{formatTime(rec.ts)}</span>
      </div>
      <div style={{fontSize: 13, color: "#333", marginBottom: 8}}>{rec.text}</div>
      {decided ? (
        <span style={{
          fontSize: 11, fontWeight: 600,
          color: rec.status === "accepted" ? "#27ae60" : "#999",
        }}>
          {rec.status === "accepted" ? "✓ Accepted" : "✕ Dismissed"}
        </span>
      ) : (
        <div style={{display: "flex", gap: 8}}>
          <button
            disabled={busy}
            onClick={() => send(true)}
            style={{fontSize: 12, padding: "4px 10px", borderRadius: 4, border: "1px solid #27ae60", background: "#eafaf1", color: "#27ae60", cursor: "pointer"}}
          >
            Accept
          </button>
          <button
            disabled={busy}
            onClick={() => send(false)}
            style={{fontSize: 12, padding: "4px 10px", borderRadius: 4, border: "1px solid #ccc", background: "#fafafa", color: "#777", cursor: "pointer"}}
          >
            Dismiss
          </button>
        </div>
      )}
    </div>
  );
}

export default function RecommendationPanel({recommendations, onFeedback}) {
  if (!recommendations || recommendations.length === 0) {
    return (
      <div style={{padding: 12, color: "#999", fontSize: 13}}>
        No recommendations yet.
      </div>
    );
  }

  return (
    <div>
      {recommendations.map(rec => (
        <RecommendationCard key={rec.id} rec={rec} onFeedback={onFeedback} />
      ))}
    </div>
  );
}
