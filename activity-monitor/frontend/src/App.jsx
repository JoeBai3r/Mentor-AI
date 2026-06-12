import React, {useEffect, useState} from "react";
import ImmediatePanel from "./components/ImmediatePanel";
import SessionsPanel from "./components/SessionsPanel";
import ProfilePanel from "./components/ProfilePanel";
import RecommendationPanel from "./components/RecommendationPanel";

const SOURCE_COLORS = {
  window: "#4a90d9",
  terminal: "#2e7d32",
  browser: "#e67e22",
  clipboard: "#8e44ad",
  filesystem: "#16a085",
  process: "#7f8c8d",
  calendar: "#c0392b",
  session: "#34495e",
};

// Order in which entity fields are shown as badges, when present.
const ENTITY_FIELDS = [
  "app", "project", "branch", "file", "domain", "category",
  "search_query", "tool", "subcommand", "exit_code", "duration_secs", "content_type",
];

const MAX_SESSIONS = 20;
const MAX_RECOMMENDATIONS = 10;

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString();
}

function EntityBadges({entities}) {
  if (!entities) return null;
  const present = ENTITY_FIELDS.filter(k => entities[k] !== undefined && entities[k] !== null && entities[k] !== "");
  if (present.length === 0) return null;
  return (
    <span>
      {present.map(k => (
        <span key={k} style={{display: "inline-block", background: "#eef", border: "1px solid #ccd", borderRadius: 4, padding: "1px 6px", marginLeft: 4, fontSize: 11}}>
          {k}: {String(entities[k])}
        </span>
      ))}
    </span>
  );
}

function SessionBoundary({event}) {
  return (
    <li style={{padding: "8px 12px", background: "#34495e", color: "#fff", fontSize: 12, fontWeight: "bold"}}>
      — {event.summary} ({event.entities?.category}) —
    </li>
  );
}

function ActivityRow({event}) {
  return (
    <li style={{padding: "6px 12px", borderBottom: "1px solid #eee", fontSize: 13}}>
      <span style={{color: "#999", fontFamily: "monospace", fontSize: 11}}>{formatTime(event.timestamp)}</span>
      {" "}
      <span style={{display: "inline-block", minWidth: 80, color: SOURCE_COLORS[event.source] || "#333", fontWeight: 600, textTransform: "uppercase", fontSize: 11}}>
        {event.source}
      </span>
      {" "}
      <span>{event.summary}</span>
      <EntityBadges entities={event.entities} />
      {event.correlated_with?.length > 0 && (
        <span style={{marginLeft: 8, color: "#c0392b", fontSize: 11}}>
          ↳ {event.correlated_with.join(", ")}
        </span>
      )}
    </li>
  );
}

function Panel({title, children}) {
  return (
    <div style={{border: "1px solid #ddd", borderRadius: 6, marginBottom: 16, background: "#fff"}}>
      <div style={{padding: "8px 12px", borderBottom: "1px solid #eee", fontWeight: 600, fontSize: 13, background: "#fafafa"}}>
        {title}
      </div>
      {children}
    </div>
  );
}

export default function App(){
  const [events, setEvents] = useState([]);
  const [status, setStatus] = useState("connecting");
  const [immediateState, setImmediateState] = useState(null);
  const [profile, setProfile] = useState(null);
  const [sessions, setSessions] = useState([]);
  const [recommendations, setRecommendations] = useState([]);

  // Initial snapshot of recently-distilled sessions; live updates arrive
  // over the websocket as new sessions close.
  useEffect(() => {
    fetch("http://localhost:3030/api/sessions/recent?limit=" + MAX_SESSIONS)
      .then(r => r.json())
      .then(data => setSessions(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  // Initial snapshot of recent recommendations; live updates arrive over the
  // websocket as the gate opens.
  useEffect(() => {
    fetch("http://localhost:3030/api/recommendations/recent?limit=" + MAX_RECOMMENDATIONS)
      .then(r => r.json())
      .then(data => setRecommendations(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  const handleFeedback = async (id, accepted) => {
    setRecommendations(prev => prev.map(r => r.id === id ? {...r, status: accepted ? "accepted" : "dismissed"} : r));
    try {
      await fetch(`http://localhost:3030/api/recommendations/${id}/feedback`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({accepted}),
      });
    } catch (_) {
      // ignore - profile/recommendation state will reconcile on next poll
    }
  };

  useEffect(()=>{
    const ws = new WebSocket("ws://localhost:3030/ws");
    ws.onopen = ()=> setStatus("connected");
    ws.onmessage = (e)=>{
      try{
        const msg = JSON.parse(e.data);
        switch (msg.type) {
          case "normalized":
            setEvents(prev => [msg.data, ...prev].slice(0, 200));
            break;
          case "immediate":
            setImmediateState(msg.data);
            break;
          case "profile":
            setProfile(msg.data);
            break;
          case "session":
            setSessions(prev => [msg.data, ...prev].slice(0, MAX_SESSIONS));
            break;
          case "recommendation":
            setRecommendations(prev => [msg.data, ...prev].slice(0, MAX_RECOMMENDATIONS));
            break;
          default:
            break;
        }
      }catch(_){
        // ignore malformed messages
      }
    };
    ws.onclose = ()=> setStatus("disconnected");
    return ()=> ws.close();
  },[]);

  const currentSession = events[0]?.session_id;

  return (
    <div style={{padding: 20, fontFamily: "sans-serif", display: "flex", gap: 20}}>
      <div style={{flex: "1 1 60%", minWidth: 0}}>
        <h1>Activity Monitor</h1>
        <p>
          Status: {status}
          {currentSession !== undefined && <> · Session #{currentSession}</>}
        </p>
        <h3>Normalized activity</h3>
        <ul style={{listStyle: "none", padding: 0, margin: 0, maxHeight: 560, overflow: "auto", background: "#fafafa", border: "1px solid #ddd"}}>
          {events.map(event => (
            event.kind === "session_boundary"
              ? <SessionBoundary key={event.seq} event={event} />
              : <ActivityRow key={event.seq} event={event} />
          ))}
        </ul>
      </div>

      <div style={{flex: "0 0 360px"}}>
        <h1 style={{visibility: "hidden", margin: 0, height: 0}}>&nbsp;</h1>
        <Panel title="Immediate activity">
          <ImmediatePanel state={immediateState} />
        </Panel>
        <Panel title="Recommendations">
          <RecommendationPanel recommendations={recommendations} onFeedback={handleFeedback} />
        </Panel>
        <Panel title="Recent sessions">
          <SessionsPanel sessions={sessions} liveSessionId={currentSession} />
        </Panel>
        <Panel title="User profile">
          <ProfilePanel profile={profile} />
        </Panel>
      </div>
    </div>
  );
}
