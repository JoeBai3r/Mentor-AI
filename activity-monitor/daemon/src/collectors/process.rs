use crate::event_bus::{Event, ListeningPort, SharedBus};
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// TCP state code for LISTEN, as found in /proc/net/tcp{,6}.
const TCP_LISTEN_STATE: &str = "0A";

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

/// Polls locally-listening TCP ports and the processes behind them. A local
/// dev server on :3000 plus a database connection tells you the user is
/// actively running/testing something - cheap to collect, useful for
/// inferring build/test/deploy phases.
pub fn start_process_collector(bus: SharedBus) {
    tokio::spawn(async move {
        let mut last: Vec<ListeningPort> = Vec::new();
        loop {
            let ports = listening_ports();
            if !same_ports(&last, &ports) {
                last = ports.clone();
                bus.publish_event(Event::ProcessActivity {
                    listening_ports: ports,
                    timestamp: now_ts(),
                });
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}

fn same_ports(a: &[ListeningPort], b: &[ListeningPort]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted: Vec<(u16, u32)> = a.iter().map(|p| (p.port, p.pid)).collect();
    let mut b_sorted: Vec<(u16, u32)> = b.iter().map(|p| (p.port, p.pid)).collect();
    a_sorted.sort_unstable();
    b_sorted.sort_unstable();
    a_sorted == b_sorted
}

fn listening_ports() -> Vec<ListeningPort> {
    let mut sockets: Vec<(u16, u64)> = Vec::new(); // (port, inode)
    sockets.extend(parse_proc_net_tcp("/proc/net/tcp"));
    sockets.extend(parse_proc_net_tcp("/proc/net/tcp6"));

    if sockets.is_empty() {
        return Vec::new();
    }

    let inode_to_pid = build_inode_pid_map();

    let mut result = Vec::new();
    for (port, inode) in sockets {
        if let Some(&pid) = inode_to_pid.get(&inode) {
            let process = process_name(pid).unwrap_or_else(|| "unknown".to_string());
            result.push(ListeningPort { port, process, pid });
        }
    }
    result
}

/// Parses local addresses in LISTEN state out of /proc/net/tcp{,6}, returning
/// (port, socket inode) pairs.
fn parse_proc_net_tcp(path: &str) -> Vec<(u16, u64)> {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for line in content.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 10 {
            continue;
        }
        if fields[3] != TCP_LISTEN_STATE {
            continue;
        }
        let local_addr = fields[1];
        let port_hex = match local_addr.split(':').nth(1) {
            Some(p) => p,
            None => continue,
        };
        let port = match u16::from_str_radix(port_hex, 16) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let inode = match fields[9].parse::<u64>() {
            Ok(i) => i,
            Err(_) => continue,
        };
        out.push((port, inode));
    }
    out
}

/// Builds a socket-inode -> pid map by scanning /proc/*/fd symlinks. Done
/// once per poll cycle to keep the cost of resolving many sockets to O(pids).
fn build_inode_pid_map() -> HashMap<u64, u32> {
    let mut map = HashMap::new();

    let entries = match fs::read_dir("/proc") {
        Ok(e) => e,
        Err(_) => return map,
    };

    for entry in entries.flatten() {
        let pid: u32 = match entry.file_name().to_str().and_then(|s| s.parse().ok()) {
            Some(p) => p,
            None => continue,
        };

        let fd_dir = format!("/proc/{}/fd", pid);
        let fds = match fs::read_dir(&fd_dir) {
            Ok(f) => f,
            Err(_) => continue,
        };

        for fd in fds.flatten() {
            if let Ok(target) = fs::read_link(fd.path()) {
                if let Some(name) = target.to_str() {
                    if let Some(inode_str) = name.strip_prefix("socket:[").and_then(|s| s.strip_suffix(']')) {
                        if let Ok(inode) = inode_str.parse::<u64>() {
                            map.entry(inode).or_insert(pid);
                        }
                    }
                }
            }
        }
    }

    map
}

fn process_name(pid: u32) -> Option<String> {
    fs::read_to_string(format!("/proc/{}/comm", pid)).ok().map(|s| s.trim().to_string())
}
