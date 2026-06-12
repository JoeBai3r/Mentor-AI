use crate::event_bus::SharedBus;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;

// Defaults to ./data, but can be overridden (e.g. when run as a Tauri sidecar,
// where the working directory is inside the app bundle and not writable/watched).
fn data_dir() -> &'static str {
    static DIR: OnceLock<String> = OnceLock::new();
    DIR.get_or_init(|| std::env::var("ACTIVITY_MONITOR_DATA_DIR").unwrap_or_else(|_| "./data".to_string()))
}

fn db_path() -> String {
    format!("{}/activity_monitor.db", data_dir())
}

pub fn start_event_store(bus: SharedBus) {
    // Initialize DB file
    let db_dir = data_dir();
    if !Path::new(db_dir).exists() {
        if let Err(e) = fs::create_dir_all(db_dir) {
            tracing::error!("failed to create data dir {}: {}", db_dir, e);
            return;
        }
    }

    let db_path = db_path();

    // Initialize schema synchronously
    if let Err(e) = init_db(&db_path) {
        tracing::error!("failed to init db {}: {}", db_path, e);
        return;
    }

    // Subscribe synchronously (before spawning) so no events published by
    // collectors or the normalizer started after this can be missed.
    let mut raw_rx = bus.subscribe();
    let mut norm_rx = bus.subscribe_normalized();

    // Persist raw collector events.
    {
        let db_path = db_path.clone();
        tokio::spawn(async move {
            loop {
                match raw_rx.recv().await {
                    Ok(val) => {
                        if let Err(e) = store_raw_event(&db_path, &val) {
                            tracing::error!("failed to store raw event: {}", e);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("store subscriber lagged: {}", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::warn!("store subscriber closed");
                        break;
                    }
                }
            }
        });
    }

    // Persist normalized session events, for the /api/events/normalized/recent endpoint.
    {
        let db_path = db_path.clone();
        tokio::spawn(async move {
            loop {
                match norm_rx.recv().await {
                    Ok(val) => {
                        if let Err(e) = store_normalized_event(&db_path, &val) {
                            tracing::error!("failed to store normalized event: {}", e);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("normalized store subscriber lagged: {}", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::warn!("normalized store subscriber closed");
                        break;
                    }
                }
            }
        });
    }
}

fn init_db(path: &str) -> rusqlite::Result<()> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "BEGIN;
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            event_type TEXT,
            raw_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS normalized_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            source TEXT,
            kind TEXT,
            raw_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS user_profile (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            profile_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS session_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            start_ts INTEGER NOT NULL,
            end_ts INTEGER NOT NULL,
            raw_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            session_id INTEGER NOT NULL,
            signal TEXT NOT NULL,
            gate TEXT NOT NULL,
            rec_type TEXT NOT NULL,
            text TEXT NOT NULL,
            status TEXT NOT NULL
        );
        COMMIT;",
    )?;
    Ok(())
}

fn store_raw_event(path: &str, v: &Value) -> rusqlite::Result<()> {
    let conn = Connection::open(path)?;
    let ts = chrono::Utc::now().timestamp() as i64;
    // Try to infer a type: if top-level key like "Window" or "Terminal" exists, use that
    let mut event_type = "unknown".to_string();
    if let Some(obj) = v.as_object() {
        if obj.len() == 1 {
            if let Some((k, _)) = obj.iter().next() { event_type = k.clone(); }
        } else {
            // common fields
            if obj.get("command").is_some() { event_type = "Terminal".to_string(); }
            else if obj.get("title").is_some() || obj.get("application").is_some() { event_type = "Window".to_string(); }
            else if obj.get("site").is_some() { event_type = "Browser".to_string(); }
        }
    }
    let raw = v.to_string();
    conn.execute(
        "INSERT INTO events (timestamp, event_type, raw_json) VALUES (?1, ?2, ?3)",
        params![ts, event_type, raw],
    )?;
    Ok(())
}

fn store_normalized_event(path: &str, v: &Value) -> rusqlite::Result<()> {
    let conn = Connection::open(path)?;
    let timestamp = v.get("timestamp").and_then(|t| t.as_i64()).unwrap_or_else(|| chrono::Utc::now().timestamp());
    let source = v.get("source").and_then(|s| s.as_str()).unwrap_or("unknown");
    let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("unknown");
    let raw = v.to_string();
    conn.execute(
        "INSERT INTO normalized_events (timestamp, source, kind, raw_json) VALUES (?1, ?2, ?3, ?4)",
        params![timestamp, source, kind, raw],
    )?;
    Ok(())
}

pub fn read_recent_events(limit: usize) -> Vec<Value> {
    read_recent_from_table("events", limit)
}

pub fn read_recent_normalized_events(limit: usize) -> Vec<Value> {
    read_recent_from_table("normalized_events", limit)
}

/// Loads the persisted user profile, or a freshly seeded "typical
/// developer" baseline if none has been saved yet (or the data dir doesn't
/// exist yet).
pub fn load_profile() -> crate::profile::UserProfile {
    if let Ok(conn) = Connection::open(db_path()) {
        let row: rusqlite::Result<String> = conn.query_row(
            "SELECT profile_json FROM user_profile WHERE id = 1",
            [],
            |row| row.get(0),
        );
        if let Ok(json) = row {
            if let Ok(profile) = serde_json::from_str(&json) {
                return profile;
            }
        }
    }
    crate::profile::UserProfile::seed_typical_developer()
}

/// Persists the user profile as a single-row JSON blob, overwriting any
/// previous value.
pub fn save_profile(profile: &crate::profile::UserProfile) -> rusqlite::Result<()> {
    let conn = Connection::open(db_path())?;
    let json = serde_json::to_string(profile).expect("UserProfile serialization is infallible");
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO user_profile (id, profile_json, updated_at) VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET profile_json = excluded.profile_json, updated_at = excluded.updated_at",
        params![json, now],
    )?;
    Ok(())
}

/// Persists a closed session's [`crate::distiller::SessionIntent`] as a JSON
/// blob row.
pub fn store_session_intent(session: &crate::distiller::SessionIntent) -> rusqlite::Result<()> {
    let conn = Connection::open(db_path())?;
    let json = serde_json::to_string(session).expect("SessionIntent serialization is infallible");
    conn.execute(
        "INSERT INTO session_intents (session_id, start_ts, end_ts, raw_json) VALUES (?1, ?2, ?3, ?4)",
        params![session.session_id as i64, session.start_ts as i64, session.end_ts as i64, json],
    )?;
    Ok(())
}

/// Reads the most recent session intents, most-recent-first.
pub fn read_recent_session_intents(limit: usize) -> Vec<crate::distiller::SessionIntent> {
    let mut out = Vec::new();
    if !Path::new(data_dir()).exists() { return out; }
    if let Ok(conn) = Connection::open(db_path()) {
        let mut stmt = match conn.prepare("SELECT raw_json FROM session_intents ORDER BY id DESC LIMIT ?1") { Ok(s) => s, Err(_) => return out };
        let mut rows = match stmt.query(params![limit as i64]) { Ok(r) => r, Err(_) => return out };
        while let Ok(Some(row)) = rows.next() {
            if let Ok(s) = row.get::<_, String>(0) {
                if let Ok(v) = serde_json::from_str(&s) { out.push(v); }
            }
        }
    }
    out
}

/// Persists a newly-generated recommendation with `status: "pending"` and
/// returns its row id, used as the `Recommendation`'s id for accept/dismiss
/// feedback.
pub fn store_recommendation(rec: &crate::recommend::Recommendation) -> rusqlite::Result<i64> {
    let conn = Connection::open(db_path())?;
    conn.execute(
        "INSERT INTO recommendations (ts, session_id, signal, gate, rec_type, text, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![rec.ts as i64, rec.session_id as i64, rec.signal, rec.gate, rec.rec_type, rec.text, rec.status],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Reads the most recent recommendations, most-recent-first.
pub fn read_recent_recommendations(limit: usize) -> Vec<crate::recommend::Recommendation> {
    let mut out = Vec::new();
    if !Path::new(data_dir()).exists() { return out; }
    if let Ok(conn) = Connection::open(db_path()) {
        let mut stmt = match conn.prepare(
            "SELECT id, ts, session_id, signal, gate, rec_type, text, status FROM recommendations ORDER BY id DESC LIMIT ?1"
        ) { Ok(s) => s, Err(_) => return out };
        let mut rows = match stmt.query(params![limit as i64]) { Ok(r) => r, Err(_) => return out };
        while let Ok(Some(row)) = rows.next() {
            out.push(crate::recommend::Recommendation {
                id: row.get(0).unwrap_or(0),
                ts: row.get::<_, i64>(1).unwrap_or(0) as u64,
                session_id: row.get::<_, i64>(2).unwrap_or(0) as u64,
                signal: row.get(3).unwrap_or_default(),
                gate: row.get(4).unwrap_or_default(),
                rec_type: row.get(5).unwrap_or_default(),
                text: row.get(6).unwrap_or_default(),
                status: row.get(7).unwrap_or_default(),
            });
        }
    }
    out
}

/// Loads a single recommendation by id, if it exists.
pub fn get_recommendation(id: i64) -> Option<crate::recommend::Recommendation> {
    let conn = Connection::open(db_path()).ok()?;
    conn.query_row(
        "SELECT id, ts, session_id, signal, gate, rec_type, text, status FROM recommendations WHERE id = ?1",
        params![id],
        |row| Ok(crate::recommend::Recommendation {
            id: row.get(0)?,
            ts: row.get::<_, i64>(1)? as u64,
            session_id: row.get::<_, i64>(2)? as u64,
            signal: row.get(3)?,
            gate: row.get(4)?,
            rec_type: row.get(5)?,
            text: row.get(6)?,
            status: row.get(7)?,
        }),
    ).ok()
}

/// Updates a recommendation's status (`"accepted"` or `"dismissed"`).
pub fn update_recommendation_status(id: i64, status: &str) -> rusqlite::Result<()> {
    let conn = Connection::open(db_path())?;
    conn.execute("UPDATE recommendations SET status = ?1 WHERE id = ?2", params![status, id])?;
    Ok(())
}

fn read_recent_from_table(table: &str, limit: usize) -> Vec<Value> {
    let mut out = Vec::new();
    if !Path::new(data_dir()).exists() { return out; }
    if let Ok(conn) = Connection::open(db_path()) {
        let mut stmt = match conn.prepare(&format!("SELECT raw_json FROM {} ORDER BY id DESC LIMIT {}", table, limit)) { Ok(s)=>s, Err(_) => return out };
        let mut rows = match stmt.query([]) { Ok(r)=>r, Err(_) => return out };
        while let Ok(Some(row)) = rows.next() {
            if let Ok(s) = row.get::<_, String>(0) {
                if let Ok(v) = serde_json::from_str(&s) { out.push(v); }
            }
        }
    }
    out
}
