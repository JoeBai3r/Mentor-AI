use std::path::{Path, PathBuf};

/// Entities extracted from a window focus/blur/launch event.
#[derive(Debug, Default, Clone)]
pub struct WindowEntities {
    pub app: Option<String>,
    pub project: Option<String>,
    pub branch: Option<String>,
    pub file: Option<String>,
}

/// Entities extracted from a terminal command.
#[derive(Debug, Default, Clone)]
pub struct CommandEntities {
    pub tool: Option<String>,
    pub subcommand: Option<String>,
    pub project: Option<String>,
    pub branch: Option<String>,
}

/// Maps raw WM_CLASS / process names to a canonical app identifier.
pub fn normalize_app_name(application: &Option<String>) -> Option<String> {
    let app = application.as_ref()?.to_lowercase();
    let canonical = match app.as_str() {
        "code" | "code - oss" | "vscodium" | "visual studio code" => "vscode",
        "google-chrome" | "chrome" | "chromium" | "chromium-browser" => "chrome",
        "firefox" | "firefox-esr" | "navigator" => "firefox",
        "gnome-terminal-server" | "gnome-terminal" | "konsole" | "xterm" | "alacritty" | "kitty" | "terminator" => "terminal",
        "slack" => "slack",
        "discord" => "discord",
        "spotify" => "spotify",
        other => other,
    };
    Some(canonical.to_string())
}

/// Project markers used to anchor a directory as a project root.
const PROJECT_MARKERS: &[&str] = &[
    ".git", "package.json", "Cargo.toml", "pyproject.toml", "go.mod", "setup.py",
];

/// Walk up from `path` looking for a directory containing a project marker.
/// Returns `None` once it reaches `$HOME` or the filesystem root without
/// finding one.
pub fn find_project_root(path: &Path) -> Option<PathBuf> {
    let home = std::env::var("HOME").ok().map(PathBuf::from);

    let mut current = path;
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

/// Reads the current branch name from `<project_root>/.git/HEAD`, e.g.
/// "ref: refs/heads/feature/auth-rework" -> "feature/auth-rework".
pub fn read_git_branch(project_root: &Path) -> Option<String> {
    let head = std::fs::read_to_string(project_root.join(".git/HEAD")).ok()?;
    let head = head.trim();
    head.strip_prefix("ref: refs/heads/").map(|s| s.to_string())
}

/// Derives project name (directory name of the project root) and current
/// git branch from a working directory path, using `.git/HEAD` for accuracy
/// rather than parsing window titles.
fn project_and_branch_from_cwd(cwd: &str) -> (Option<String>, Option<String>) {
    let root = match find_project_root(Path::new(cwd)) {
        Some(r) => r,
        None => return (None, None),
    };
    let project = root.file_name().map(|n| n.to_string_lossy().to_string());
    let branch = read_git_branch(&root);
    (project, branch)
}

/// Parses a window title/cwd into structured entities. Title formats vary
/// wildly by application, so only a few common patterns (notably VS Code's
/// "file - project - Visual Studio Code") are special-cased; project root
/// and branch are derived from `cwd` via `.git` for accuracy.
pub fn parse_window_title(application: &Option<String>, title: &Option<String>, cwd: &Option<String>) -> WindowEntities {
    let app = normalize_app_name(application);
    let mut project = None;
    let mut file = None;

    if let Some(title) = title {
        // Split on common title separators (hyphen or em-dash surrounded by spaces).
        let parts: Vec<&str> = title.split(" - ").flat_map(|s| s.split(" \u{2013} ")).map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

        match app.as_deref() {
            Some("vscode") => {
                // "filename - project - Visual Studio Code" or "project - Visual Studio Code"
                if parts.len() >= 3 {
                    file = Some(parts[0].trim_start_matches('\u{25cf}').trim().to_string());
                    project = Some(parts[parts.len() - 2].to_string());
                } else if parts.len() == 2 {
                    project = Some(parts[0].to_string());
                }
            }
            _ => {
                // Generic apps: the first segment is often the open document/page.
                if parts.len() >= 2 {
                    file = Some(parts[0].to_string());
                }
            }
        }
    }

    let mut branch = None;
    if let Some(cwd) = cwd {
        let (cwd_project, cwd_branch) = project_and_branch_from_cwd(cwd);
        if project.is_none() {
            project = cwd_project;
        }
        branch = cwd_branch;
    }

    WindowEntities { app, project, branch, file }
}

/// Tools that take a subcommand as their first non-flag argument, e.g.
/// `git commit`, `cargo build`, `npm run dev`.
const MULTI_COMMAND_TOOLS: &[&str] = &[
    "git", "npm", "yarn", "pnpm", "cargo", "docker", "docker-compose", "kubectl", "go", "python", "python3", "pip", "pip3", "make", "systemctl",
];

/// Parses a shell command into tool/subcommand, and derives project/branch
/// from `cwd`. Leading environment variable assignments (`FOO=bar cmd ...`)
/// are skipped when identifying the tool.
pub fn parse_command(command: &str, cwd: &Option<String>) -> CommandEntities {
    let tokens: Vec<&str> = command.split_whitespace().collect();

    let mut idx = 0;
    while idx < tokens.len() && tokens[idx].contains('=') && !tokens[idx].starts_with('-') {
        idx += 1;
    }

    let tool = tokens.get(idx).map(|s| s.to_string());
    let subcommand = if tool.as_deref().map(|t| MULTI_COMMAND_TOOLS.contains(&t)).unwrap_or(false) {
        tokens[idx + 1..].iter().find(|t| !t.starts_with('-')).map(|s| s.to_string())
    } else {
        None
    };

    let (project, branch) = match cwd {
        Some(cwd) => project_and_branch_from_cwd(cwd),
        None => (None, None),
    };

    CommandEntities { tool, subcommand, project, branch }
}

/// Heuristic: does this command look like it's running a test suite?
pub fn is_test_command(entities: &CommandEntities, command: &str) -> bool {
    if entities.subcommand.as_deref() == Some("test") {
        return true;
    }
    let lower = command.to_lowercase();
    ["pytest", "jest", "vitest", "go test", "rspec", "mocha"].iter().any(|m| lower.contains(m))
}

/// Heuristic: does this command look like a `git commit`?
pub fn is_git_commit(entities: &CommandEntities) -> bool {
    entities.tool.as_deref() == Some("git") && entities.subcommand.as_deref() == Some("commit")
}

/// Extracts a domain from a URL string, e.g. "https://docs.rs/foo" -> "docs.rs".
pub fn extract_domain(url: &str) -> Option<String> {
    let without_scheme = url.split("://").nth(1).unwrap_or(url);
    let host = without_scheme.split(['/', '?', '#']).next()?;
    let host = host.split('@').next_back()?; // strip userinfo, if any
    let host = host.split(':').next()?; // strip port
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}
