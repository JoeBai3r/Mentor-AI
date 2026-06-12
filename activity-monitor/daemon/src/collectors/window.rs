use crate::event_bus::SharedBus;
use x11rb::protocol::xproto::ConnectionExt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task;

/// X11-based window collector using x11rb. Runs in a blocking thread and publishes events to the bus.
pub fn start_window_collector(bus: SharedBus) {
    // Spawn a blocking task because x11rb uses blocking primitives
    task::spawn_blocking(move || {
        if let Err(e) = run_x11_collector(bus) {
            tracing::error!("window collector error: {}", e);
        }
    });
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn run_x11_collector(bus: SharedBus) -> Result<(), Box<dyn std::error::Error>> {
    use x11rb::connection::Connection;
    use x11rb::protocol::xproto::{AtomEnum, ChangeWindowAttributesAux, EventMask};
    use x11rb::protocol::xproto::ConnectionExt;
    use x11rb::rust_connection::RustConnection;
    use x11rb::protocol::Event as XEvent;

    let (conn, screen_num) = RustConnection::connect(None)?;
    let setup = conn.setup();
    let screen = &setup.roots[screen_num];
    let root = screen.root;

    // Intern atoms we will use
    let net_active_atom = conn.intern_atom(false, b"_NET_ACTIVE_WINDOW")?.reply()?.atom;
    let net_wm_name = conn.intern_atom(false, b"_NET_WM_NAME")?.reply()?.atom;
    let utf8_string = conn.intern_atom(false, b"UTF8_STRING")?.reply()?.atom;
    let wm_name = conn.intern_atom(false, b"WM_NAME")?.reply()?.atom;
    let wm_class = conn.intern_atom(false, b"WM_CLASS")?.reply()?.atom;
    let net_wm_pid = conn.intern_atom(false, b"_NET_WM_PID")?.reply()?.atom;

    // Select for PropertyChange and SubstructureNotify (MapNotify/DestroyNotify) on root
    let aux = ChangeWindowAttributesAux::new().event_mask(EventMask::PROPERTY_CHANGE | EventMask::SUBSTRUCTURE_NOTIFY);
    conn.change_window_attributes(root, &aux)?;
    conn.flush()?;

    tracing::info!("X11 window collector started (root {}). Listening for events...", root);

    // Tracks the currently-focused window so we can emit a "blur" event with
    // its dwell time once focus moves elsewhere.
    let mut current_focus: Option<FocusState> = None;

    loop {
        let ev = conn.wait_for_event()?;
        match ev {
            XEvent::PropertyNotify(prop) => {
                if prop.atom == net_active_atom {
                    // Active window changed
                    if let Ok(reply) = conn.get_property(false, root, net_active_atom, AtomEnum::WINDOW, 0, 1)?.reply() {
                        if let Some(mut v) = reply.value32() {
                            if let Some(win) = v.next() {
                                let title = get_window_title(&conn, win, net_wm_name, utf8_string, wm_name).ok().flatten();
                                let app = get_window_class(&conn, win, wm_class).ok().flatten();
                                let pid = get_window_pid(&conn, win, net_wm_pid).ok().flatten();
                                let cwd = pid.and_then(get_process_cwd);
                                let now = now_ts();

                                // Emit dwell time for the window that just lost focus.
                                if let Some(prev) = current_focus.take() {
                                    let ev = crate::event_bus::Event::Window {
                                        event_type: "blur".to_string(),
                                        application: prev.application,
                                        title: prev.title,
                                        cwd: prev.cwd,
                                        duration_secs: Some(now.saturating_sub(prev.started_at)),
                                        timestamp: now,
                                    };
                                    bus.publish_event(ev);
                                }

                                let ev = crate::event_bus::Event::Window {
                                    event_type: "focus".to_string(),
                                    application: app.clone(),
                                    title: title.clone(),
                                    cwd: cwd.clone(),
                                    duration_secs: None,
                                    timestamp: now,
                                };
                                bus.publish_event(ev);

                                current_focus = Some(FocusState { application: app, title, cwd, started_at: now });
                            }
                        }
                    }
                }
            }
            XEvent::MapNotify(map_ev) => {
                let win = map_ev.window;
                let title = get_window_title(&conn, win, net_wm_name, utf8_string, wm_name).ok().flatten();
                let app = get_window_class(&conn, win, wm_class).ok().flatten();
                let pid = get_window_pid(&conn, win, net_wm_pid).ok().flatten();
                let cwd = pid.and_then(get_process_cwd);
                let ev = crate::event_bus::Event::Window {
                    event_type: "launch".to_string(),
                    application: app,
                    title,
                    cwd,
                    duration_secs: None,
                    timestamp: now_ts(),
                };
                bus.publish_event(ev);
            }
            // DestroyNotify carries no application/title info by the time it
            // fires, so it isn't useful for inferring activity - skip it.
            _ => {}
        }
    }
}

/// State for the window currently holding focus, used to compute dwell time.
struct FocusState {
    application: Option<String>,
    title: Option<String>,
    cwd: Option<String>,
    started_at: u64,
}

fn get_window_title(conn: &x11rb::rust_connection::RustConnection, window: u32, net_wm_name: u32, utf8_string: u32, wm_name: u32) -> Result<Option<String>, Box<dyn std::error::Error>> {
    use x11rb::protocol::xproto::AtomEnum;
    // Prefer _NET_WM_NAME (UTF8_STRING), fall back to WM_NAME
    if let Ok(reply) = conn.get_property(false, window, net_wm_name, utf8_string, 0, 1024)?.reply() {
        if let Some(bytes) = reply.value8() {
            if let Ok(s) = String::from_utf8(bytes.collect::<Vec<u8>>()) { return Ok(Some(s)); }
        }
    }
    if let Ok(reply) = conn.get_property(false, window, wm_name, AtomEnum::STRING, 0, 1024)?.reply() {
        if let Some(bytes) = reply.value8() {
            if let Ok(s) = String::from_utf8(bytes.collect::<Vec<u8>>()) { return Ok(Some(s)); }
        }
    }
    Ok(None)
}

fn get_window_class(conn: &x11rb::rust_connection::RustConnection, window: u32, wm_class_atom: u32) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if let Ok(reply) = conn.get_property(false, window, wm_class_atom, x11rb::protocol::xproto::AtomEnum::STRING, 0, 1024)?.reply() {
        if !reply.value.is_empty() {
            // WM_CLASS is two null-terminated strings: instance\0class\0
            let bytes = &reply.value;
            if let Some(pos) = bytes.iter().position(|&b| b == 0) {
                let class_bytes = &bytes[pos+1..];
                if let Some(pos2) = class_bytes.iter().position(|&b| b == 0) {
                    if let Ok(s) = String::from_utf8(class_bytes[..pos2].to_vec()) {
                        return Ok(Some(s));
                    }
                }
            }
        }
    }
    Ok(None)
}

fn get_window_pid(conn: &x11rb::rust_connection::RustConnection, window: u32, pid_atom: u32) -> Result<Option<u32>, Box<dyn std::error::Error>> {
    if let Ok(reply) = conn.get_property(false, window, pid_atom, x11rb::protocol::xproto::AtomEnum::CARDINAL, 0, 1)?.reply() {
        if let Some(mut v) = reply.value32() { return Ok(v.next()); }
    }
    Ok(None)
}

/// Resolve a process's current working directory via /proc, e.g. to surface
/// which project directory a focused editor or terminal is rooted in.
fn get_process_cwd(pid: u32) -> Option<String> {
    std::fs::read_link(format!("/proc/{}/cwd", pid))
        .ok()
        .map(|p| p.to_string_lossy().to_string())
}
