use crate::event_bus::{Event, SharedBus};
use notify::event::ModifyKind;
use notify::{EventKind, RecursiveMode, Watcher};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// How many project roots to watch at once. Roots are evicted LRU-style as
/// the user moves between projects (tracked via terminal/window `cwd`).
const MAX_WATCHED_ROOTS: usize = 3;

/// Minimum gap between emitted events for the same path, to avoid flooding
/// on editors that write a file multiple times per save.
const DEBOUNCE: Duration = Duration::from_secs(2);

const IGNORE_DIRS: &[&str] = &[
    "node_modules", ".git", "target", "dist", "build", "__pycache__", ".venv", "venv", ".next", ".cache",
];

/// Extensions of files the daemon itself writes (its sqlite store), which
/// should never be reported as user activity.
const IGNORE_EXTENSIONS: &[&str] = &["db", "db-journal", "db-wal", "db-shm"];

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Watches the filesystem under recently-active project directories (derived
/// from Window/Terminal `cwd`) and publishes file create/modify/remove
/// events. Combined with terminal cwd, this anchors activity to a specific
/// project and catches "saved a new file" task transitions.
/// Resolves the daemon's own data directory (where it stores its sqlite db),
/// so the filesystem collector doesn't report its own writes as user activity.
fn data_dir_canonical() -> Option<PathBuf> {
    let dir = std::env::var("ACTIVITY_MONITOR_DATA_DIR").unwrap_or_else(|_| "./data".to_string());
    std::fs::canonicalize(dir).ok()
}

pub fn start_filesystem_collector(bus: SharedBus) {
    let data_dir = data_dir_canonical();

    tokio::spawn(async move {
        let mut rx = bus.subscribe();
        let (tx, mut watch_rx) = tokio::sync::mpsc::unbounded_channel::<notify::Event>();

        let mut watcher = match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res {
                let _ = tx.send(event);
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!("filesystem watcher init failed: {}", e);
                return;
            }
        };

        let mut watched: VecDeque<PathBuf> = VecDeque::new();
        let mut last_emit: HashMap<PathBuf, Instant> = HashMap::new();

        loop {
            tokio::select! {
                res = rx.recv() => {
                    match res {
                        Ok(val) => {
                            if let Some(cwd) = extract_cwd(&val) {
                                let Some(root) = project_root(&cwd) else { continue };
                                if let Some(pos) = watched.iter().position(|p| p == &root) {
                                    // already watched - bump to most-recently-used
                                    watched.remove(pos);
                                    watched.push_back(root);
                                } else {
                                    if watched.len() >= MAX_WATCHED_ROOTS {
                                        if let Some(old) = watched.pop_front() {
                                            let _ = watcher.unwatch(&old);
                                        }
                                    }
                                    if watcher.watch(&root, RecursiveMode::Recursive).is_ok() {
                                        tracing::info!("filesystem collector watching {}", root.display());
                                        watched.push_back(root);
                                    }
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                Some(fs_event) = watch_rx.recv() => {
                    handle_fs_event(&bus, &fs_event, &watched, &mut last_emit, data_dir.as_deref());
                }
            }
        }
    });
}

/// Events are serialized as `{"Window": {...}}` / `{"Terminal": {...}}`.
fn extract_cwd(v: &serde_json::Value) -> Option<PathBuf> {
    let obj = v.as_object()?;
    if obj.len() != 1 { return None; }
    let (variant, inner) = obj.iter().next()?;
    if variant != "Window" && variant != "Terminal" { return None; }
    let cwd = inner.as_object()?.get("cwd")?.as_str()?;
    if cwd.is_empty() { return None; }
    Some(PathBuf::from(cwd))
}

/// Project markers that anchor a directory as a project root worth watching.
const PROJECT_MARKERS: &[&str] = &[
    ".git", "package.json", "Cargo.toml", "pyproject.toml", "go.mod", "setup.py",
];

/// Walk up from `cwd` looking for a directory containing a project marker
/// (`.git`, `package.json`, etc). Returns `None` if no marker is found within
/// a few levels - notably, this avoids falling back to watching `cwd` itself
/// when it's something broad like the user's home directory.
fn project_root(cwd: &Path) -> Option<PathBuf> {
    let home = std::env::var("HOME").ok().map(PathBuf::from);

    let mut current = cwd;
    for _ in 0..8 {
        if home.as_deref() == Some(current) {
            return None;
        }
        if PROJECT_MARKERS.iter().any(|m| current.join(m).exists()) {
            return Some(current.to_path_buf());
        }
        match current.parent() {
            Some(p) => current = p,
            None => break,
        }
    }
    None
}

fn is_ignored(path: &Path, data_dir: Option<&Path>) -> bool {
    if let Some(data_dir) = data_dir {
        if path.starts_with(data_dir) {
            return true;
        }
    }

    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        if IGNORE_EXTENSIONS.contains(&ext) {
            return true;
        }
    }

    path.components().any(|c| {
        if let std::path::Component::Normal(name) = c {
            if let Some(s) = name.to_str() {
                return IGNORE_DIRS.contains(&s);
            }
        }
        false
    })
}

fn handle_fs_event(bus: &SharedBus, event: &notify::Event, watched: &VecDeque<PathBuf>, last_emit: &mut HashMap<PathBuf, Instant>, data_dir: Option<&Path>) {
    let kind = match event.kind {
        EventKind::Create(_) => "created",
        EventKind::Modify(ModifyKind::Data(_)) => "modified",
        EventKind::Modify(ModifyKind::Name(_)) => "renamed",
        EventKind::Remove(_) => "removed",
        _ => return,
    };

    for path in &event.paths {
        if is_ignored(path, data_dir) { continue; }

        let now = Instant::now();
        if let Some(last) = last_emit.get(path) {
            if now.duration_since(*last) < DEBOUNCE { continue; }
        }
        last_emit.insert(path.clone(), now);
        if last_emit.len() > 2000 {
            let cutoff = Instant::now() - Duration::from_secs(60);
            last_emit.retain(|_, t| *t > cutoff);
        }

        let project = watched.iter().find(|root| path.starts_with(root)).map(|r| r.to_string_lossy().to_string());

        let ev = Event::FileChange {
            path: path.to_string_lossy().to_string(),
            kind: kind.to_string(),
            project,
            timestamp: now_ts(),
        };
        bus.publish_event(ev);
    }
}
