use std::{
    collections::HashMap,
    fs,
    io,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use std::sync::{Arc, Mutex};

// ── Configuration ──────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Config {
    #[serde(default)]
    general: GeneralConfig,
    #[serde(default)]
    keybindings: KeybindingsConfig,
    #[serde(default)]
    layout: LayoutConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
enum ThemeType {
    #[default]
    Default,
    Catppuccin,
    Tokyonight,
    Nord,
}

#[derive(Debug, Clone)]
struct ThemeColors {
    active_border: Color,
    inactive_border: Color,
    path_fg: Color,
    path_bg_search: Color,
    selection_bg: Color,
    status_running: Color,
    status_done: Color,
    status_error: Color,
    url_fg: Color,
}

impl ThemeType {
    fn colors(&self) -> ThemeColors {
        match self {
            ThemeType::Default => ThemeColors {
                active_border: Color::Yellow,
                inactive_border: Color::Reset,
                path_fg: Color::Cyan,
                path_bg_search: Color::Yellow,
                selection_bg: Color::DarkGray,
                status_running: Color::Green,
                status_done: Color::Cyan,
                status_error: Color::Red,
                url_fg: Color::Cyan,
            },
            ThemeType::Catppuccin => ThemeColors {
                active_border: Color::Rgb(203, 166, 247),
                inactive_border: Color::Rgb(147, 153, 178),
                path_fg: Color::Rgb(137, 180, 250),
                path_bg_search: Color::Rgb(249, 226, 175),
                selection_bg: Color::Rgb(88, 91, 112),
                status_running: Color::Rgb(166, 227, 161),
                status_done: Color::Rgb(137, 180, 250),
                status_error: Color::Rgb(243, 139, 168),
                url_fg: Color::Rgb(245, 194, 231),
            },
            ThemeType::Tokyonight => ThemeColors {
                active_border: Color::Rgb(187, 154, 247),
                inactive_border: Color::Rgb(86, 95, 137),
                path_fg: Color::Rgb(122, 162, 247),
                path_bg_search: Color::Rgb(224, 175, 104),
                selection_bg: Color::Rgb(65, 72, 104),
                status_running: Color::Rgb(158, 206, 106),
                status_done: Color::Rgb(122, 162, 247),
                status_error: Color::Rgb(247, 118, 142),
                url_fg: Color::Rgb(157, 124, 205),
            },
            ThemeType::Nord => ThemeColors {
                active_border: Color::Rgb(136, 192, 208),
                inactive_border: Color::Rgb(76, 86, 106),
                path_fg: Color::Rgb(143, 188, 187),
                path_bg_search: Color::Rgb(235, 203, 139),
                selection_bg: Color::Rgb(67, 76, 94),
                status_running: Color::Rgb(163, 190, 140),
                status_done: Color::Rgb(136, 192, 208),
                status_error: Color::Rgb(191, 97, 106),
                url_fg: Color::Rgb(180, 142, 173),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct GeneralConfig {
    #[serde(default)]
    exclude_drives: Vec<String>,
    #[serde(default)]
    custom_paths: Vec<String>,
    #[serde(default)]
    log_persistence: bool,
    #[serde(default)]
    theme: ThemeType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct KeybindingsConfig {
    #[serde(default = "default_up")]
    up: String,
    #[serde(default = "default_down")]
    down: String,
    #[serde(default = "default_enter")]
    enter: String,
    #[serde(default = "default_back")]
    back: String,
    #[serde(default = "default_run")]
    run: String,
    #[serde(default = "default_stop")]
    stop: String,
    #[serde(default = "default_settings")]
    settings: String,
    #[serde(default = "default_quit")]
    quit: String,
    #[serde(default = "default_toggle_layout")]
    toggle_layout: String,
    #[serde(default = "default_toggle_sidebar")]
    toggle_sidebar: String,
    #[serde(default = "default_toggle_split")]
    toggle_split: String,
    #[serde(default = "default_next_pane")]
    next_pane: String,
    #[serde(default = "default_prev_pane")]
    prev_pane: String,
    #[serde(default = "default_expand_left")]
    expand_left: String,
    #[serde(default = "default_expand_right")]
    expand_right: String,
    #[serde(default = "default_expand_up")]
    expand_up: String,
    #[serde(default = "default_expand_down")]
    expand_down: String,
    #[serde(default = "default_clear_log")]
    clear_log: String,
    #[serde(default = "default_cycle_log_view")]
    cycle_log_view: String,
    #[serde(default = "default_export_log")]
    export_log: String,
}

impl Default for KeybindingsConfig {
    fn default() -> Self {
        Self {
            up: default_up(),
            down: default_down(),
            enter: default_enter(),
            back: default_back(),
            run: default_run(),
            stop: default_stop(),
            settings: default_settings(),
            quit: default_quit(),
            toggle_layout: default_toggle_layout(),
            toggle_sidebar: default_toggle_sidebar(),
            toggle_split: default_toggle_split(),
            next_pane: default_next_pane(),
            prev_pane: default_prev_pane(),
            expand_left: default_expand_left(),
            expand_right: default_expand_right(),
            expand_up: default_expand_up(),
            expand_down: default_expand_down(),
            clear_log: default_clear_log(),
            cycle_log_view: default_cycle_log_view(),
            export_log: default_export_log(),
        }
    }
}

fn default_up()             -> String { "w".to_string() }
fn default_down()           -> String { "s".to_string() }
fn default_enter()          -> String { "d".to_string() }
fn default_back()           -> String { "a".to_string() }
fn default_run()            -> String { "r".to_string() }
fn default_stop()           -> String { "x".to_string() }
fn default_quit()           -> String { "q".to_string() }
fn default_settings()       -> String { "f".to_string() }
fn default_toggle_layout()  -> String { "l".to_string() }
fn default_toggle_sidebar() -> String { "s".to_string() }
fn default_toggle_split()   -> String { "o".to_string() }
fn default_next_pane()      -> String { "tab".to_string() }
fn default_prev_pane()      -> String { "backtab".to_string() }
fn default_expand_left()  -> String { "alt-left".to_string() }
fn default_expand_right() -> String { "alt-right".to_string() }
fn default_expand_up()    -> String { "alt-up".to_string() }
fn default_expand_down()  -> String { "alt-down".to_string() }
fn default_clear_log()    -> String { "c".to_string() }
fn default_cycle_log_view()-> String { "v".to_string() }
fn default_export_log()     -> String { "e".to_string() }
// ── Layout AST ───────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum SplitDir { Vertical, Horizontal }

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum LayoutNode {
    Pane(Pane),
    Split {
        dir: SplitDir,
        ratio: u16,
        a: Box<LayoutNode>,
        b: Box<LayoutNode>,
    },
    Empty,
}

impl LayoutNode {
    /// Renders the tree and populates the given vector with (Rect, Pane)
    pub fn render(&self, area: Rect, app: &App, out: &mut Vec<(Rect, Pane)>) {
        match self {
            LayoutNode::Pane(p) => {
                if app.is_visible(*p) { out.push((area, *p)); }
            }
            LayoutNode::Split { dir, ratio, a, b } => {
                let a_has = a.has_visible(app);
                let b_has = b.has_visible(app);
                
                if a_has && b_has {
                    let d = if *dir == SplitDir::Horizontal { Direction::Horizontal } else { Direction::Vertical };
                    let chunks = Layout::default()
                        .direction(d)
                        .constraints([Constraint::Percentage(*ratio), Constraint::Percentage(100 - *ratio)])
                        .split(area);
                    a.render(chunks[0], app, out);
                    b.render(chunks[1], app, out);
                } else if a_has {
                    a.render(area, app, out);
                } else if b_has {
                    b.render(area, app, out);
                }
            }
            LayoutNode::Empty => {}
        }
    }

    pub fn has_visible(&self, app: &App) -> bool {
        match self {
            LayoutNode::Pane(p) => app.is_visible(*p),
            LayoutNode::Split { a, b, .. } => a.has_visible(app) || b.has_visible(app),
            LayoutNode::Empty => false,
        }
    }

    pub fn resize_boundary(&mut self, target: Pane, dx: i16, dy: i16) -> bool {
        match self {
            LayoutNode::Pane(p) => *p == target,
            LayoutNode::Split { dir, ratio, a, b } => {
                let mut found = false;
                if a.resize_boundary(target, dx, dy) {
                    found = true;
                    if *dir == SplitDir::Horizontal && dx != 0 {
                        *ratio = (*ratio as i16 + dx).clamp(10, 90) as u16;
                    }
                    if *dir == SplitDir::Vertical && dy != 0 {
                        *ratio = (*ratio as i16 + dy).clamp(10, 90) as u16;
                    }
                }
                if b.resize_boundary(target, dx, dy) {
                    found = true;
                    if *dir == SplitDir::Horizontal && dx != 0 {
                        *ratio = (*ratio as i16 + dx).clamp(10, 90) as u16;
                    }
                    if *dir == SplitDir::Vertical && dy != 0 {
                        *ratio = (*ratio as i16 + dy).clamp(10, 90) as u16;
                    }
                }
                found
            }
            LayoutNode::Empty => false,
        }
    }

    pub fn extract(&mut self, target: Pane) -> bool {
        match self {
            LayoutNode::Pane(p) => {
                if *p == target { *self = LayoutNode::Empty; return true; }
                false
            }
            LayoutNode::Split { a, b, .. } => a.extract(target) || b.extract(target),
            LayoutNode::Empty => false,
        }
    }

    pub fn toggle_split_dir(&mut self, target: Pane) -> bool {
        match self {
            LayoutNode::Pane(p) => *p == target,
            LayoutNode::Split { dir, a, b, .. } => {
                // If the target is inside one of our branches, check if it's the immediate child
                let found_in_a = a.toggle_split_dir(target);
                let found_in_b = b.toggle_split_dir(target);
                
                // If it was found below us, but it didn't trigger a flip yet, we flip ourselves
                if found_in_a && matches!(**a, LayoutNode::Pane(_)) {
                    *dir = if *dir == SplitDir::Horizontal { SplitDir::Vertical } else { SplitDir::Horizontal };
                    return false; // Stop bubbling
                }
                if found_in_b && matches!(**b, LayoutNode::Pane(_)) {
                    *dir = if *dir == SplitDir::Horizontal { SplitDir::Vertical } else { SplitDir::Horizontal };
                    return false; // Stop bubbling
                }
                
                found_in_a || found_in_b
            }
            LayoutNode::Empty => false,
        }
    }

    pub fn cleanup(self) -> LayoutNode {
        match self {
            LayoutNode::Split { dir, ratio, a, b } => {
                let clean_a = a.cleanup();
                let clean_b = b.cleanup();
                if matches!(clean_a, LayoutNode::Empty) { return clean_b; }
                if matches!(clean_b, LayoutNode::Empty) { return clean_a; }
                LayoutNode::Split { dir, ratio, a: Box::new(clean_a), b: Box::new(clean_b) }
            }
            other => other,
        }
    }

    pub fn swap_pane(&mut self, p1: Pane, p2: Pane) {
        match self {
            LayoutNode::Pane(p) => {
                if *p == p1 { *p = p2; }
                else if *p == p2 { *p = p1; }
            }
            LayoutNode::Split { a, b, .. } => {
                a.swap_pane(p1, p2);
                b.swap_pane(p1, p2);
            }
            LayoutNode::Empty => {}
        }
    }
}

// ── Layout configuration (persisted to .polyrunner.toml) ──────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LayoutConfig {
    #[serde(default = "lc_default_tree")]
    pub tree: LayoutNode,
    #[serde(default = "bool_true")]  show_logs: bool,
    #[serde(default = "bool_true")]  show_running: bool,
    #[serde(default = "bool_true")]  show_sidebar: bool,
}

fn lc_default_tree() -> LayoutNode {
    LayoutNode::Split {
        dir: SplitDir::Horizontal, // Left/Right cols
        ratio: 50,
        a: Box::new(LayoutNode::Split {
            dir: SplitDir::Vertical, // Top/Bot left
            ratio: 50,
            a: Box::new(LayoutNode::Pane(Pane::Projects)),
            b: Box::new(LayoutNode::Pane(Pane::Sidebar)),
        }),
        b: Box::new(LayoutNode::Split {
            dir: SplitDir::Vertical, // Top/Bot right
            ratio: 50,
            a: Box::new(LayoutNode::Pane(Pane::Logs)),
            b: Box::new(LayoutNode::Pane(Pane::Running)),
        }),
    }
}

fn bool_true() -> bool { true }

impl Default for LayoutConfig {
    fn default() -> Self {
        Self {
            tree: lc_default_tree(),
            show_logs: true, show_running: true, show_sidebar: true,
        }
    }
}

impl Config {
    fn load() -> (Self, PathBuf) {
        let local_config = PathBuf::from(".polyrunner.toml");
        let exe_config = std::env::current_exe().ok().map(|p| p.parent().unwrap().join(".polyrunner.toml"));
        let home_config = dirs::home_dir().map(|h| h.join(".polyrunner.toml"));

        let config_path = if local_config.exists() {
            Some(local_config)
        } else if let Some(p) = exe_config.clone() {
            if p.exists() { Some(p) } else { None }
        } else if let Some(h) = home_config.clone() {
            if h.exists() { Some(h) } else { None }
        } else {
            None
        };

        if let Some(path) = config_path {
            if let Ok(content) = fs::read_to_string(&path) {
                match toml::from_str::<Config>(&content) {
                    Ok(cfg) => (cfg, path),
                    Err(e) => {
                        eprintln!("Error loading config from {:?}: {}", path, e);
                        (Self::default(), path)
                    }
                }
            } else {
                (Self::default(), path)
            }
        } else {
            // No config found, we generate one in the executable directory or HOME
            let target_path = exe_config.or(home_config).unwrap_or_else(|| PathBuf::from(".polyrunner.toml"));
            let default_cfg = Self::default();
            let toml_string = toml::to_string(&default_cfg).unwrap_or_default();
            let _ = fs::write(&target_path, toml_string);
            (default_cfg, target_path)
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            general: GeneralConfig::default(),
            keybindings: KeybindingsConfig::default(),
            layout: LayoutConfig::default(),
        }
    }
}

// ── Domain types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum PackageManager {
    Bun,
    Pnpm,
    Yarn,
    Npm,
}

impl PackageManager {
    /// Detect which package manager a Node project uses from its lockfiles or package.json.
    fn detect(path: &Path) -> Self {
        if let Ok(content) = std::fs::read_to_string(path.join("package.json")) {
            if content.contains("\"packageManager\": \"pnpm") {
                return PackageManager::Pnpm;
            } else if content.contains("\"packageManager\": \"bun") {
                return PackageManager::Bun;
            } else if content.contains("\"packageManager\": \"yarn") {
                return PackageManager::Yarn;
            } else if content.contains("\"packageManager\": \"npm") {
                return PackageManager::Npm;
            }
        }

        if path.join("pnpm-workspace.yaml").exists() || path.join("pnpm-lock.yaml").exists() {
            PackageManager::Pnpm
        } else if path.join("bun.lockb").exists() || path.join("bun.lock").exists() {
            PackageManager::Bun
        } else if path.join("yarn.lock").exists() {
            PackageManager::Yarn
        } else {
            PackageManager::Npm
        }
    }

    fn name(&self) -> &str {
        match self {
            PackageManager::Bun  => "bun",
            PackageManager::Pnpm => "pnpm",
            PackageManager::Yarn => "yarn",
            PackageManager::Npm  => "npm",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ProjectType {
    Node(PackageManager),
    Python,
    Rust,
    Go,
    PHP,
    Docker,
    Drive,
    ParentDir,
    Unknown,
}

impl ProjectType {
    fn tag(&self) -> String {
        match self {
            ProjectType::Node(pm) => pm.name().to_string(),
            ProjectType::Python   => "py ".to_string(),
            ProjectType::Rust     => "rs ".to_string(),
            ProjectType::Go       => "go ".to_string(),
            ProjectType::PHP      => "php".to_string(),
            ProjectType::Docker   => "dkr".to_string(),
            ProjectType::Drive    => "drv".to_string(),
            ProjectType::ParentDir => ".. ".to_string(),
            ProjectType::Unknown  => " ? ".to_string(),
        }
    }

    fn color(&self) -> Color {
        match self {
            ProjectType::Node(_) => Color::Yellow,
            ProjectType::Python  => Color::Blue,
            ProjectType::Rust    => Color::Red,
            ProjectType::Go      => Color::Cyan,
            ProjectType::PHP     => Color::Magenta,
            ProjectType::Docker  => Color::LightBlue,
            ProjectType::Drive   => Color::Magenta,
            ProjectType::ParentDir => Color::DarkGray,
            ProjectType::Unknown => Color::DarkGray,
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
enum ProcessStatus {
    Idle,
    Running,
    Exited(i32),
    Failed(String),
}

impl ProcessStatus {
    fn label(&self) -> String {
        match self {
            ProcessStatus::Idle => "idle".to_string(),
            ProcessStatus::Running => "▶ running".to_string(),
            ProcessStatus::Exited(0) => "✓ done (0)".to_string(),
            ProcessStatus::Exited(code) => format!("✗ exited({})", code),
            ProcessStatus::Failed(e) => format!("✗ {}", e),
        }
    }

    fn color(&self, theme: &ThemeColors) -> Color {
        match self {
            ProcessStatus::Running => theme.status_running,
            ProcessStatus::Exited(0) => theme.status_done,
            ProcessStatus::Exited(_) | ProcessStatus::Failed(_) => theme.status_error,
            ProcessStatus::Idle => Color::DarkGray,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: String,
    pub project: String,
    pub message: String,
}

#[derive(Debug, Clone)]
struct Project {
    name: String,
    path: PathBuf,
    project_type: ProjectType,
    selected: bool,
}

// ── Messages from background tasks → main loop ──────────────────────────────

/// Sent from background to main loop when deps are missing and user confirmation is needed.
struct InstallRequest {
    /// Human-readable description for the popup, e.g. "'my-app': node_modules missing"
    description: String,
    /// Channel to send the user's answer (true = yes, false = no)
    confirm_tx: oneshot::Sender<bool>,
}

enum BgMsg {
    Status(String, ProcessStatus),
    Url(String, String),
    Pid(String, u32),
    Log(String, String),       // (Project Name, Log Line)
    Metrics(String, f32, u64), // (Project Name, cpu_percent, mem_bytes)
    Health(String, bool),      // (Project Name, is_healthy)
    /// Background task requests user confirmation before installing dependencies.
    NeedsInstall(InstallRequest),
}

/// Returns a HH:MM:SS timestamp string for the current local time.
fn timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // seconds within a day (UTC approximation — good enough for TUI display)
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

// ── Drive detection (Windows) ────────────────────────────────────────────────

// ── Package-manager / runtime detection ─────────────────────────────────────

/// Inspect a project directory and return `(program, args)` to launch it.
/// Returns `None` if the project type has no known run command.
fn detect_command(project_type: &ProjectType, path: &Path) -> Option<(String, Vec<String>)> {
    match project_type {
        ProjectType::Node(pm) => {
            // Read package.json to find an available script rather than forcing "dev"
            let mut script_to_run = "dev".to_string();
            if let Ok(content) = std::fs::read_to_string(path.join("package.json")) {
                if content.contains("\"dev\":") || content.contains("\"dev\" :") {
                    script_to_run = "dev".to_string();
                } else if content.contains("\"start\":") || content.contains("\"start\" :") {
                    script_to_run = "start".to_string();
                } else if content.contains("\"build\":") || content.contains("\"build\" :") {
                    script_to_run = "build".to_string();
                }
            }

            let args = vec!["run".to_string(), script_to_run];
            Some((pm.name().to_string(), args))
        }
        ProjectType::Python => {
            let is_fastapi = std::fs::read_to_string(path.join("main.py"))
                .map(|s| s.contains("FastAPI") || s.contains("fastapi"))
                .unwrap_or(false);

            let mut args = Vec::new();

            let has_uv = path.join(".venv").exists() || path.join("uv.lock").exists() || {
                let toml_path = path.join("pyproject.toml");
                toml_path.exists() && std::fs::read_to_string(toml_path).map(|s| s.contains("[tool.uv]")).unwrap_or(false)
            };

            let prog = if has_uv {
                args.push("run".to_string());
                "uv".to_string()
            } else if path.join("poetry.lock").exists() {
                args.push("run".to_string());
                "poetry".to_string()
            } else if path.join("Pipfile").exists() {
                args.push("run".to_string());
                "pipenv".to_string()
            } else if is_fastapi {
                "uvicorn".to_string()
            } else {
                #[cfg(windows)]
                { "python".to_string() }
                #[cfg(not(windows))]
                { "python3".to_string() }
            };

            if prog == "uv" || prog == "poetry" || prog == "pipenv" {
                if is_fastapi {
                    args.extend(["uvicorn".to_string(), "main:app".to_string(), "--reload".to_string()]);
                } else {
                    args.extend(["python".to_string(), "main.py".to_string()]);
                }
            } else if prog == "uvicorn" {
                args.extend(["main:app".to_string(), "--reload".to_string()]);
            } else {
                args.push("main.py".to_string());
            }

            Some((prog, args))
        }
        ProjectType::Rust => {
            Some(("cargo".to_string(), vec!["run".to_string()]))
        }
        ProjectType::Go => {
            // Check for a main.go or any *.go file as the entry point
            Some(("go".to_string(), vec!["run".to_string(), ".".to_string()]))
        }
        ProjectType::PHP => {
            // Laravel uses artisan
            Some(("php".to_string(), vec!["artisan".to_string(), "serve".to_string()]))
        }
        ProjectType::Docker => {
            // Prefer compose.yaml (new spec), fall back to docker-compose.yml
            let compose_file = if path.join("compose.yaml").exists() {
                "compose.yaml"
            } else {
                "docker-compose.yml"
            };
            Some((
                "docker".to_string(),
                vec!["compose".to_string(), "-f".to_string(), compose_file.to_string(), "up".to_string()],
            ))
        }
        _ => None,
    }
}

/// Returns a list of available drive roots on Windows (e.g. C:\, D:\).
fn get_drives(exclude: &[String]) -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        let mut drives = Vec::new();
        // GetLogicalDrives returns a bitmask: bit 0 = A:, bit 1 = B:, bit 2 = C:, ...
        let mask = unsafe { winapi::um::fileapi::GetLogicalDrives() };
        for i in 0..26u32 {
            if mask & (1 << i) != 0 {
                let letter = (b'A' + i as u8) as char;
                let path_str = format!("{}:\\", letter);
                if !exclude.contains(&path_str) {
                    drives.push(PathBuf::from(path_str));
                }
            }
        }
        drives
    }
    #[cfg(not(windows))]
    {
        // On Unix there is only one root
        vec![PathBuf::from("/")]
    }
}

/// True if `path` is a filesystem root (e.g. C:\ on Windows, / on Unix).
#[allow(dead_code)]
fn is_root(path: &Path) -> bool {
    path.parent().is_none() || path.parent() == Some(Path::new(""))
}

// ── App state ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Pane {
    Sidebar,
    Projects,
    Logs,
    Running,
}

struct App {
    all_projects: Vec<Project>,
    projects: Vec<Project>,
    filter: String,
    is_filtering: bool,
    cursor: usize,
    list_state: ListState,
    drives: Vec<Project>,
    drives_cursor: usize,
    drives_state: ListState,
    show_sidebar: bool,
    active_pane: Pane,
    statuses: HashMap<String, ProcessStatus>,
    urls: HashMap<String, String>,
    pids: HashMap<String, u32>,
    metrics: HashMap<String, (f32, u64)>,  // (cpu%, mem_bytes)
    health: HashMap<String, bool>,          // true = healthy
    log: Vec<LogEntry>,
    log_filter: Option<String>,
    scan_dir: PathBuf,
    config: Config,
    config_path: PathBuf,
    auto_restart: bool,
    restart_cooldowns: HashMap<String, std::time::Instant>,
    // ── Layout state ────────────────────────────────────────────────────────────
    layout_tree: LayoutNode,
    rendered_panes: Vec<(Rect, Pane)>, // Cached from last render
    show_logs: bool,
    show_running: bool,
    bg_tx: mpsc::UnboundedSender<BgMsg>,
    /// Pending install confirmation request from a background task.
    pending_install: Option<InstallRequest>,
}

impl App {
    fn new(scan_dir: PathBuf, config: Config, config_path: PathBuf, bg_tx: mpsc::UnboundedSender<BgMsg>) -> Self {
        let mut drives: Vec<Project> = get_drives(&config.general.exclude_drives)
            .into_iter()
            .map(|p| {
                let name = p.display().to_string();
                Project {
                    name,
                    path: p,
                    project_type: ProjectType::Drive,
                    selected: false,
                }
            })
            .collect();
            
        for path_str in &config.general.custom_paths {
            let p = PathBuf::from(path_str.replace("~", &dirs::home_dir().unwrap_or_default().display().to_string()));
            if p.exists() {
                drives.push(Project {
                    name: p.display().to_string(),
                    path: p,
                    project_type: ProjectType::Drive,
                    selected: false,
                });
            }
        }

        let mut drives_state = ListState::default();
        if !drives.is_empty() {
            drives_state.select(Some(0));
        }

        let mut app = Self {
            all_projects: Vec::new(),
            projects: Vec::new(),
            filter: String::new(),
            is_filtering: false,
            cursor: 0,
            list_state: ListState::default(),
            drives,
            drives_cursor: 0,
            drives_state,
            show_sidebar: config.layout.show_sidebar,
            active_pane: Pane::Projects,
            statuses: HashMap::new(),
            urls: HashMap::new(),
            pids: HashMap::new(),
            metrics: HashMap::new(),
            health: HashMap::new(),
            log: Vec::new(),
            log_filter: None,
            scan_dir,
            layout_tree: config.layout.tree.clone(),
            rendered_panes: Vec::new(),
            show_logs:       config.layout.show_logs,
            show_running:    config.layout.show_running,
            config,
            config_path,
            auto_restart: true,
            restart_cooldowns: HashMap::new(),
            bg_tx,
            pending_install: None,
        };
        app.refresh_list();
        app
    }

    fn add_system_log(&mut self, msg: String) {
        self.log.push(LogEntry {
            timestamp: timestamp(),
            project: "System".to_string(),
            message: msg,
        });
    }

    fn clear_log(&mut self) {
        self.log.clear();
    }

    fn cycle_log_view(&mut self) {
        let mut active_projects: Vec<String> = self.statuses.iter()
            .filter(|(_, s)| **s != ProcessStatus::Idle)
            .map(|(k, _)| k.clone())
            .collect();
        active_projects.sort();
        
        let mut views = vec![None];
        for p in active_projects {
            views.push(Some(p));
        }
        
        let current_idx = views.iter().position(|v| v == &self.log_filter).unwrap_or(0);
        let next_idx = (current_idx + 1) % views.len();
        self.log_filter = views[next_idx].clone();
    }

    fn export_current_log(&mut self) {
        let filtered_logs: Vec<String> = self.log.iter()
            .filter(|e| {
                if let Some(filter) = &self.log_filter {
                    &e.project == filter || e.project == "System"
                } else {
                    true
                }
            })
            .map(|e| format!("[{}] {:>12} | {}", e.timestamp, e.project, e.message))
            .collect();
            
        let content = filtered_logs.join("\n");
        if let Ok(_) = std::fs::write("exported_log.txt", content) {
            self.add_system_log("💾 Log exported to exported_log.txt".to_string());
        }
    }

    fn save_layout(&mut self) {
        self.config.layout.tree = self.layout_tree.clone();
        self.config.layout.show_logs = self.show_logs;
        self.config.layout.show_running = self.show_running;
        self.config.layout.show_sidebar = self.show_sidebar;
        
        let toml_string = toml::to_string(&self.config).unwrap_or_default();
        let _ = std::fs::write(&self.config_path, toml_string);
    }

    fn refresh_list(&mut self) {
        self.all_projects = scan_projects(&self.scan_dir);
        self.apply_filter();
    }

    fn apply_filter(&mut self) {
        if self.filter.is_empty() {
            self.projects = self.all_projects.clone();
        } else {
            let f = self.filter.to_lowercase();
            self.projects = self.all_projects
                .iter()
                .filter(|p| p.name.to_lowercase().contains(&f) || p.name == "..")
                .cloned()
                .collect();
        }
        self.cursor = 0;
        self.list_state.select(if self.projects.is_empty() { None } else { Some(0) });
    }

    fn move_up(&mut self) {
        match self.active_pane {
            Pane::Projects => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.list_state.select(Some(self.cursor));
                }
            }
            Pane::Sidebar => {
                if self.drives_cursor > 0 {
                    self.drives_cursor -= 1;
                    self.drives_state.select(Some(self.drives_cursor));
                }
            }
            Pane::Logs | Pane::Running => {}
        }
    }

    fn move_down(&mut self) {
        match self.active_pane {
            Pane::Projects => {
                if !self.projects.is_empty() && self.cursor < self.projects.len() - 1 {
                    self.cursor += 1;
                    self.list_state.select(Some(self.cursor));
                }
            }
            Pane::Sidebar => {
                if !self.drives.is_empty() && self.drives_cursor < self.drives.len() - 1 {
                    self.drives_cursor += 1;
                    self.drives_state.select(Some(self.drives_cursor));
                }
            }
            Pane::Logs | Pane::Running => {}
        }
    }

    fn toggle_selection(&mut self) {
        if self.active_pane == Pane::Projects {
            if let Some(p) = self.projects.get_mut(self.cursor) {
                if p.project_type != ProjectType::Drive && p.name != ".." {
                    p.selected = !p.selected;
                }
            }
        }
    }

    /// (dead code kept for possible future keybinding)
    #[allow(dead_code)]
    fn toggle_layout(&mut self) {
        // cycle the focused panel clockwise
        self.layout_active_panel();
    }

    // (old grid resize_active_panel removed, using AST now)

    #[allow(dead_code)]
    fn toggle_running_panel(&mut self) {
        self.show_running = !self.show_running;
        if !self.show_running && self.active_pane == Pane::Running {
            self.active_pane = Pane::Projects;
        }
    }

    // ── Grid helpers ──────────────────────────────────────────

    // ── AST Grid helpers ──────────────────────────────────────────

    fn is_visible(&self, pane: Pane) -> bool {
        match pane {
            Pane::Projects => true,
            Pane::Logs => self.show_logs,
            Pane::Running => self.show_running,
            Pane::Sidebar => self.show_sidebar,
        }
    }

    /// Moves focus geographically based on cached rendered rects.
    fn swap_with_neighbor(&mut self, dx: i16, dy: i16) {
        let active = self.active_pane;
        let mut active_rect = None;
        for (rect, pane) in &self.rendered_panes {
            if *pane == active { active_rect = Some(*rect); break; }
        }
        let Some(arect) = active_rect else { return; };
        
        let cx = arect.x as i32 + (arect.width as i32 / 2);
        let cy = arect.y as i32 + (arect.height as i32 / 2);
        
        // Push target coordinates
        let tx = cx + (dx as i32 * arect.width as i32);
        let ty = cy + (dy as i32 * arect.height as i32);

        // Find the panel whose center is closest to (tx, ty)
        let mut best_pane = None;
        let mut min_dist = i32::MAX;

        for (rect, pane) in &self.rendered_panes {
            if *pane == active { continue; }
            let px = rect.x as i32 + (rect.width as i32 / 2);
            let py = rect.y as i32 + (rect.height as i32 / 2);
            let dist = (px - tx).abs() + (py - ty).abs();
            
            // Basic directional filter
            let valid = match (dx, dy) {
                (1, 0) => px > cx,
                (-1, 0) => px < cx,
                (0, 1) => py > cy,
                (0, -1) => py < cy,
                _ => false,
            };
            if valid && dist < min_dist {
                min_dist = dist;
                best_pane = Some(*pane);
            }
        }

        if let Some(target) = best_pane {
            self.layout_tree.swap_pane(active, target);
        }
    }

    fn resize_active_panel(&mut self, dx: i16, dy: i16) {
        let pane = self.active_pane;
        self.layout_tree.resize_boundary(pane, dx, dy);
    }

    /// O key: Toggle layout Split Direction (Vert/Horiz) of active pane's parent split
    fn toggle_split_direction(&mut self) {
        let target = self.active_pane;
        self.layout_tree.toggle_split_dir(target);
    }

    /// L key: Reset Layout to Default 2x2
    fn layout_active_panel(&mut self) {
        self.layout_tree = lc_default_tree();
    }

    /// Arrow keys: swap the active panel with its direct neighbor in that direction.
    /// If the slot is empty, it simply moves the panel to the empty slot.
    fn move_active_panel(&mut self, code: KeyCode) {
        let (dx, dy): (i16, i16) = match code {
            KeyCode::Left  => (-1,  0),
            KeyCode::Right => ( 1,  0),
            KeyCode::Up    => ( 0, -1),
            KeyCode::Down  => ( 0,  1),
            _              => return,
        };
        self.swap_with_neighbor(dx, dy);
    }

    /// H key: hide active panel (H on Projects restores all).
    fn hide_active_panel(&mut self) {
        match self.active_pane {
            Pane::Projects => {
                self.show_sidebar = true;
                self.show_logs    = true;
                self.show_running = true;
            }
            Pane::Sidebar => { self.show_sidebar = false; self.active_pane = Pane::Projects; }
            Pane::Logs    => { self.show_logs    = false; self.active_pane = Pane::Projects; }
            Pane::Running => {
                self.show_running = false;
                self.active_pane = if self.show_logs { Pane::Logs } else { Pane::Projects };
            }
        }
    }

    #[allow(dead_code)]
    fn toggle_sidebar(&mut self) {
        self.show_sidebar = !self.show_sidebar;
        if !self.show_sidebar && self.active_pane == Pane::Sidebar {
            self.active_pane = Pane::Projects;
        }
    }

    fn next_pane(&mut self) {
        self.active_pane = match self.active_pane {
            Pane::Sidebar  => Pane::Projects,
            Pane::Projects => if self.show_logs { Pane::Logs } else if self.show_running { Pane::Running } else if self.show_sidebar { Pane::Sidebar } else { Pane::Projects },
            Pane::Logs     => if self.show_running { Pane::Running } else if self.show_sidebar { Pane::Sidebar } else { Pane::Projects },
            Pane::Running  => if self.show_sidebar { Pane::Sidebar } else { Pane::Projects },
        };
    }

    fn prev_pane(&mut self) {
        self.active_pane = match self.active_pane {
            Pane::Sidebar  => if self.show_running { Pane::Running } else if self.show_logs { Pane::Logs } else { Pane::Projects },
            Pane::Projects => if self.show_sidebar { Pane::Sidebar } else if self.show_running { Pane::Running } else if self.show_logs { Pane::Logs } else { Pane::Projects },
            Pane::Logs     => Pane::Projects,
            Pane::Running  => if self.show_logs { Pane::Logs } else { Pane::Projects },
        };
    }

    /// D key: enter the highlighted entry
    fn enter_item(&mut self) {
        match self.active_pane {
            Pane::Projects => {
                if let Some(p) = self.projects.get(self.cursor) {
                    let new_dir = p.path.clone();
                    self.scan_dir = new_dir;
                    self.refresh_list();
                }
            }
            Pane::Sidebar => {
                if let Some(p) = self.drives.get(self.drives_cursor) {
                    let new_dir = p.path.clone();
                    self.scan_dir = new_dir;
                    self.refresh_list();
                    self.active_pane = Pane::Projects;
                }
            }
            Pane::Logs | Pane::Running => {}
        }
    }

    /// A key: go back
    fn go_back(&mut self) {
        if let Some(parent) = self.scan_dir.parent().map(|p| p.to_path_buf()) {
            if !parent.as_os_str().is_empty() {
                self.scan_dir = parent;
                self.refresh_list();
            }
        }
    }

    /// Kill all selected running processes.
    fn kill_selected(&mut self) {
        let projects_to_kill: Vec<Project> = self.projects.iter().filter(|p| p.selected).cloned().collect();
        for project in projects_to_kill {
            if self.statuses.get(&project.name) != Some(&ProcessStatus::Running) {
                continue;
            }
            if let Some(&pid) = self.pids.get(&project.name) {
                kill_process(pid);
                self.add_system_log(format!("⏹ '{}' killed (PID {})", project.name, pid));
                self.statuses
                    .insert(project.name.clone(), ProcessStatus::Exited(-1));
                self.pids.remove(&project.name);
            } else {
                self.add_system_log(format!("⚠ '{}' PID unknown, can't kill", project.name));
            }
        }
    }
    
    /// Open the settings file in the system default editor.
    fn open_settings(&mut self) {
        let path = &self.config_path;
        #[cfg(windows)]
        {
            let _ = std::process::Command::new("cmd")
                .args(["/c", "start", "", &path.display().to_string()])
                .spawn();
        }
        #[cfg(target_os = "macos")]
        {
            let _ = std::process::Command::new("open")
                .arg(path)
                .spawn();
        }
        #[cfg(all(not(windows), not(target_os = "macos")))]
        {
            let _ = std::process::Command::new("xdg-open")
                .arg(path)
                .spawn();
        }
        self.add_system_log(format!("📝 Opening settings: {:?}", path));
    }

    /// Apply messages received from background tasks (non-blocking drain).
    fn apply_bg_msgs(&mut self, rx: &mut mpsc::UnboundedReceiver<BgMsg>) {
        while let Ok(msg) = rx.try_recv() {
            match msg {
                BgMsg::Status(name, s) => {
                    let old_status = self.statuses.get(&name).cloned();
                    self.statuses.insert(name.clone(), s.clone());
                    
                    if self.auto_restart {
                        if let (Some(ProcessStatus::Running), ProcessStatus::Exited(code)) = (old_status, &s) {
                            if *code != 0 {
                                let now = Instant::now();
                                let last_restart = self.restart_cooldowns.get(&name).cloned();
                                if last_restart.map(|t| now.duration_since(t).as_secs() >= 3).unwrap_or(true) {
                                    self.restart_cooldowns.insert(name.clone(), now);
                                    if let Some(p) = self.all_projects.iter().find(|p| p.name == name).cloned() {
                                        self.add_system_log(format!("🔄 Auto-restarting crashed service: {}", name));
                                        let bg_tx = self.bg_tx.clone();
                                        tokio::spawn(async move {
                                            tokio::time::sleep(Duration::from_secs(3)).await;
                                            launch_project(p, bg_tx).await;
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                BgMsg::Url(name, url) => {
                    self.urls.insert(name.clone(), url.clone());
                    self.add_system_log(format!("🌐 '{}' → {}", name, url));
                }
                BgMsg::Pid(name, pid) => {
                    self.pids.insert(name, pid);
                }
                BgMsg::Log(name, log_line) => {
                    self.log.push(LogEntry {
                        timestamp: timestamp(),
                        project: name.clone(),
                        message: log_line.clone(),
                    });
                    if self.config.general.log_persistence {
                        let formatted = format!("[{}][{}] {}", timestamp(), name, log_line);
                        if let Ok(mut f) = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(".polyrunner.log")
                        {
                            use std::io::Write;
                            let _ = writeln!(f, "{}", formatted);
                        }
                    }
                }
                BgMsg::Metrics(name, cpu, mem) => {
                    self.metrics.insert(name, (cpu, mem));
                }
                BgMsg::Health(name, healthy) => {
                    let was = self.health.get(&name).copied();
                    self.health.insert(name.clone(), healthy);
                    // Only log on state change
                    if was != Some(healthy) {
                        let icon = if healthy { "✅" } else { "🔴" };
                        self.add_system_log(format!("{} '{}' health changed", icon, name));
                    }
                }
                BgMsg::NeedsInstall(req) => {
                    // Show confirmation popup; drop any previous one
                    self.add_system_log(format!("📦 Install required: {}", req.description));
                    self.pending_install = Some(req);
                }
            }
        }
    }

    fn launch_selected(&mut self) {
        let projects_to_launch: Vec<Project> = self.projects.iter().filter(|p| p.selected).cloned().collect();
        for project in projects_to_launch {
            if self.statuses.get(&project.name) == Some(&ProcessStatus::Running) {
                self.add_system_log(format!("⚠ '{}' is already running", project.name));
                continue;
            }

            let name = project.name.clone();
            let path = project.path.clone();

            let Some((cmd, args)) = detect_command(&project.project_type, &path) else {
                self.add_system_log(format!("⚠ '{}' has no known run command", name));
                continue;
            };
            let tx = self.bg_tx.clone();

            self.statuses.insert(name.clone(), ProcessStatus::Running);
            self.add_system_log(format!("▶ '{}' → {} {}", name, cmd, args.join(" ")));

            tokio::spawn(launch_project(project.clone(), tx));
        }
    }
}

async fn launch_project(project: Project, tx: mpsc::UnboundedSender<BgMsg>) {
    let name = project.name.clone();
    let path = project.path.clone();

    let Some((cmd, args)) = detect_command(&project.project_type, &path) else {
        return;
    };

    // ── Dependency check: ask user before installing ────────────────────────
    let install_spec: Option<(String, Vec<String>)> = match &project.project_type {
        ProjectType::Node(_) if !path.join("node_modules").exists() => {
            Some((cmd.clone(), vec!["install".to_string()]))
        }
        ProjectType::Python if {
            let needs_venv = !path.join(".venv").exists() && !path.join("venv").exists();
            let has_reqs = path.join("requirements.txt").exists()
                || path.join("pyproject.toml").exists()
                || path.join("poetry.lock").exists()
                || path.join("Pipfile").exists();
            needs_venv && has_reqs
        } => {
            // Determine install sub-command based on detected runner
            let (install_cmd, install_args) = if path.join("uv.lock").exists() || path.join("pyproject.toml").exists() {
                ("uv".to_string(), vec!["sync".to_string()])
            } else if path.join("poetry.lock").exists() {
                ("poetry".to_string(), vec!["install".to_string()])
            } else if path.join("Pipfile").exists() {
                ("pipenv".to_string(), vec!["install".to_string()])
            } else {
                ("pip".to_string(), vec!["install".to_string(), "-r".to_string(), "requirements.txt".to_string()])
            };
            Some((install_cmd, install_args))
        }
        ProjectType::PHP if !path.join("vendor").exists() && path.join("composer.json").exists() => {
            Some(("composer".to_string(), vec!["install".to_string()]))
        }
        _ => None,
    };

    if let Some((install_cmd, install_args)) = install_spec {
        // Ask for user confirmation via the TUI popup
        let description = format!(
            "'{}' is missing dependencies. Run: {} {}?",
            name,
            install_cmd,
            install_args.join(" ")
        );

        let (confirm_tx, confirm_rx) = oneshot::channel::<bool>();
        let _ = tx.send(BgMsg::NeedsInstall(InstallRequest { description, confirm_tx }));

        // Wait for the user's answer (Y/N from the TUI)
        match confirm_rx.await {
            Ok(true) => {
                let _ = tx.send(BgMsg::Log(name.clone(), format!("⚙ Installing: {} {}", install_cmd, install_args.join(" "))));

                #[cfg(windows)]
                let mut install_process = tokio::process::Command::new("cmd");
                #[cfg(windows)]
                install_process.arg("/C").arg(&install_cmd).args(&install_args).current_dir(&path);

                #[cfg(not(windows))]
                let mut install_process = tokio::process::Command::new(&install_cmd);
                #[cfg(not(windows))]
                install_process.args(&install_args).current_dir(&path);

                match install_process.output().await {
                    Ok(out) if out.status.success() => {
                        let _ = tx.send(BgMsg::Log(name.clone(), "✅ Dependencies installed successfully. Starting...".to_string()));
                    }
                    Ok(out) => {
                        let err_msg = String::from_utf8_lossy(&out.stderr);
                        let _ = tx.send(BgMsg::Log(name.clone(), format!("❌ Install failed: {}", err_msg.trim())));
                        let _ = tx.send(BgMsg::Status(name.clone(), ProcessStatus::Failed("Install failed".to_string())));
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(BgMsg::Log(name.clone(), format!("❌ Install error: {}", e)));
                        let _ = tx.send(BgMsg::Status(name.clone(), ProcessStatus::Failed("Install error".to_string())));
                        return;
                    }
                }
            }
            Ok(false) | Err(_) => {
                let _ = tx.send(BgMsg::Log(name.clone(), "⏭ Install skipped by user. Starting anyway...".to_string()));
            }
        }
    }

    #[cfg(windows)]
    let mut command = tokio::process::Command::new("cmd");
    #[cfg(windows)]
    command.arg("/C").arg(&cmd).args(&args);

    #[cfg(not(windows))]
    let mut command = tokio::process::Command::new(&cmd);
    #[cfg(not(windows))]
    command.args(&args);

    let spawn_result = command
        .current_dir(&path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();

    match spawn_result {
        Ok(mut child) => {
            if let Some(pid) = child.id() {
                let _ = tx.send(BgMsg::Pid(name.clone(), pid));

                // ── Metrics polling task ──────────────────────
                let tx_metrics = tx.clone();
                let name_metrics = name.clone();
                let target_pid = pid;
                tokio::spawn(async move {
                    use sysinfo::{Pid, System};
                    let mut sys = System::new_all();
                    loop {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        sys.refresh_all();
                        let p = sys.process(Pid::from_u32(target_pid));
                        if let Some(proc_) = p {
                            let cpu = proc_.cpu_usage();
                            let mem = proc_.memory(); // bytes
                            let _ = tx_metrics.send(BgMsg::Metrics(name_metrics.clone(), cpu, mem));
                        } else {
                            break;
                        }
                    }
                });
            }

            // ── Health check task ───
            let url_slot: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
            let url_slot_writer = Arc::clone(&url_slot);
            let tx_health = tx.clone();
            let name_health = name.clone();
            tokio::spawn(async move {
                let mut retries = 0;
                let url = loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    retries += 1;
                    if let Ok(guard) = url_slot_writer.lock() {
                        if let Some(u) = guard.clone() {
                            break u;
                        }
                    }
                    if retries > 30 { return; }
                };
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(3))
                    .build()
                    .unwrap_or_default();
                loop {
                    let healthy = client.get(&url).send().await
                        .map(|r| r.status().is_success() || r.status().as_u16() < 500)
                        .unwrap_or(false);
                    let _ = tx_health.send(BgMsg::Health(name_health.clone(), healthy));
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            });

            if let Some(stdout) = child.stdout.take() {
                let tx2 = tx.clone();
                let n2 = name.clone();
                let url_slot2 = Arc::clone(&url_slot);
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stdout).lines();
                    let mut found_url = false;
                    while let Ok(Some(line)) = lines.next_line().await {
                        let cleaned = String::from_utf8(strip_ansi_escapes::strip(&line)).unwrap_or(line.clone());
                        let _ = tx2.send(BgMsg::Log(n2.clone(), cleaned.clone()));
                        if !found_url {
                            if let Some(url) = extract_url(&cleaned) {
                                let _ = tx2.send(BgMsg::Url(n2.clone(), url.clone()));
                                if let Ok(mut slot) = url_slot2.lock() {
                                    *slot = Some(url);
                                }
                                found_url = true;
                            }
                        }
                    }
                });
            }

            if let Some(stderr) = child.stderr.take() {
                let tx3 = tx.clone();
                let n3 = name.clone();
                let url_slot3 = Arc::clone(&url_slot);
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    let mut found_url = false;
                    while let Ok(Some(line)) = lines.next_line().await {
                        let cleaned = String::from_utf8(strip_ansi_escapes::strip(&line)).unwrap_or(line.clone());
                        let _ = tx3.send(BgMsg::Log(n3.clone(), cleaned.clone()));
                        if !found_url {
                            if let Some(url) = extract_url(&cleaned) {
                                let _ = tx3.send(BgMsg::Url(n3.clone(), url.clone()));
                                if let Ok(mut slot) = url_slot3.lock() {
                                    *slot = Some(url);
                                }
                                found_url = true;
                            }
                        }
                    }
                });
            }

            match child.wait().await {
                Ok(status) => {
                    let code = status.code().unwrap_or(-1);
                    let _ = tx.send(BgMsg::Status(name.clone(), ProcessStatus::Exited(code)));
                }
                Err(e) => {
                    let _ = tx.send(BgMsg::Status(name.clone(), ProcessStatus::Failed(e.to_string())));
                }
            }
        }
        Err(e) => {
            let _ = tx.send(BgMsg::Status(name.clone(), ProcessStatus::Failed(e.to_string())));
        }
    }
}

// ── Kill helper ──────────────────────────────────────────────────────────────

fn kill_process(pid: u32) {
    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F", "/T"])
            .output();
    }
    #[cfg(not(windows))]
    {
        let _ = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .output();
    }
}

// ── Project scanner ──────────────────────────────────────────────────────────

fn scan_projects(base_dir: &Path) -> Vec<Project> {
    let mut projects = Vec::new();
    
    if let Some(parent) = base_dir.parent() {
        if !parent.as_os_str().is_empty() {
            projects.push(Project {
                name: "..".to_string(),
                path: parent.to_path_buf(),
                project_type: ProjectType::ParentDir,
                selected: false,
            });
        }
    }

    let Ok(entries) = std::fs::read_dir(base_dir) else {
        return projects;
    };

    let mut scanned_projects: Vec<Project> = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if !path.is_dir() {
                return None;
            }
            let name = path.file_name()?.to_str()?.to_string();

            let project_type = if path.join("package.json").exists() {
                ProjectType::Node(PackageManager::detect(&path))
            } else if path.join("requirements.txt").exists()
                || path.join("main.py").exists()
                || path.join("pyproject.toml").exists()
            {
                ProjectType::Python
            } else if path.join("Cargo.toml").exists() {
                ProjectType::Rust
            } else if path.join("go.mod").exists() {
                ProjectType::Go
            } else if path.join("artisan").exists() {
                ProjectType::PHP
            } else if path.join("docker-compose.yml").exists()
                || path.join("compose.yaml").exists()
            {
                ProjectType::Docker
            } else {
                ProjectType::Unknown
            };

            Some(Project {
                name,
                path,
                project_type,
                selected: false,
            })
        })
        .collect();

    scanned_projects.sort_by(|a, b| a.name.cmp(&b.name));
    projects.extend(scanned_projects);
    projects
}

// ── URL / port extractor ────────────────────────────────────────────────────

fn extract_url(line: &str) -> Option<String> {
    for proto in &["https://", "http://"] {
        if let Some(idx) = line.find(proto) {
            let rest = &line[idx..];
            let end = rest
                .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ')'))
                .unwrap_or(rest.len());
            let url = &rest[..end];
            if url.len() > proto.len() {
                return Some(url.to_string());
            }
        }
    }
    if let Some(idx) = line.find("localhost:") {
        let port_str = &line[idx + "localhost:".len()..];
        let end = port_str
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(port_str.len());
        if end > 0 {
            return Some(format!("http://localhost:{}", &port_str[..end]));
        }
    }
    for prefix in &["0.0.0.0:", "127.0.0.1:"] {
        if let Some(idx) = line.find(prefix) {
            let port_str = &line[idx + prefix.len()..];
            let end = port_str
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(port_str.len());
            if end > 0 {
                return Some(format!("http://localhost:{}", &port_str[..end]));
            }
        }
    }
    None
}

// ── Rendering ────────────────────────────────────────────────────────────────

fn render(frame: &mut ratatui::Frame, app: &mut App) {
    let area = frame.area();

    // ── Pre-calculate footer height for responsiveness ─────────────────────
    let kb = &app.config.keybindings;
    let label_style = Style::default().fg(Color::DarkGray);
    let theme = app.config.general.theme.colors();
    let key_style = Style::default().fg(theme.active_border).add_modifier(Modifier::BOLD);

    let hide_label = if app.active_pane == Pane::Projects { "Restore all" } else { "Hide panel" };
    let items = [
        ("Up", kb.up.to_uppercase()), ("Down", kb.down.to_uppercase()),
        ("Enter", kb.enter.to_uppercase()), ("Pane", "Tab".to_string()),
        ("Sel", "SPACE".to_string()), ("Back", kb.back.to_uppercase()),
        ("Run", kb.run.to_uppercase()), ("Stop", kb.stop.to_uppercase()),
        ("Reset", "L".to_string()), ("H", hide_label.to_string()),
        ("Orient", kb.toggle_split.to_uppercase()),
        ("Move", "Arrows".to_string()), ("Resize", "Alt+Arrows".to_string()),
        ("Filter Log", kb.cycle_log_view.to_uppercase()), ("Clear Log", kb.clear_log.to_uppercase()),
        ("Export Log", kb.export_log.to_uppercase()),
        ("Config", kb.settings.to_uppercase()), ("Quit", kb.quit.to_uppercase()),
    ];

    let mut footer_lines = Vec::new();
    let mut current_line = Vec::new();
    let mut current_w = 0;
    let max_w = (area.width.saturating_sub(4)) as usize;

    for (label, key) in items {
        let item_str = format!(" {}: {}  ", label, key);
        if !current_line.is_empty() && current_w + item_str.len() > max_w {
            footer_lines.push(Line::from(current_line));
            current_line = Vec::new();
            current_w = 0;
        }
        current_line.push(Span::styled(format!(" {}:", label), label_style));
        current_line.push(Span::styled(key, key_style));
        current_line.push(Span::raw("   "));
        current_w += item_str.len();
    }
    if !current_line.is_empty() {
        footer_lines.push(Line::from(current_line));
    }
    let footer_height = (footer_lines.len() as u16 + 2).min(area.height / 3);

    let mut outer_constraints = vec![Constraint::Length(3)]; // path
    outer_constraints.push(Constraint::Min(5)); // main content
    outer_constraints.push(Constraint::Length(footer_height));

    let outer = Layout::default().direction(Direction::Vertical).constraints(outer_constraints).split(area);

    let main_area = outer[1];
    let footer_area = outer.last().copied().unwrap();

    let mut panes = Vec::new();
    app.layout_tree.render(main_area, app, &mut panes);
    app.rendered_panes = panes.clone();

    let get_area = |p: Pane| panes.iter().find(|(_, tp)| *tp == p).map(|(r, _)| *r);
    let projects_area    = get_area(Pane::Projects).unwrap_or(main_area);
    let log_area_opt     = get_area(Pane::Logs);
    let running_area_opt = get_area(Pane::Running);
    let drives_area_opt  = get_area(Pane::Sidebar);

    fn pane_border<'a>(title: &'a str, is_active: bool, theme: &ThemeColors) -> ratatui::widgets::Block<'a> {
        let mut b = Block::default().borders(Borders::ALL).title(title.to_string());
        if is_active {
            b = b.border_style(Style::default().fg(theme.active_border).add_modifier(Modifier::BOLD));
        } else {
            b = b.border_style(Style::default().fg(theme.inactive_border));
        }
        b
    }



    // ── Header bar ─────────────────────────────────────────────────────────
    let active_pos = match app.active_pane {
        Pane::Projects => "Projects",
        Pane::Logs     => "Log",
        Pane::Running  => "Running",
        Pane::Sidebar  => "Drives",
    };
    let path_title = format!(" {} | AST Tree Mode ", active_pos);
    let dir_text = if app.is_filtering {
        format!("  🔍 Search: {} ", app.filter)
    } else if !app.filter.is_empty() {
        format!("  🔍 Filtered: {} (Press / to edit)  ", app.filter)
    } else {
        format!("  📁 {}  ", app.scan_dir.display())
    };

    let path_bar = Paragraph::new(dir_text)
        .block(Block::default().borders(Borders::ALL).title(path_title).border_style(Style::default().fg(theme.inactive_border)))
        .style(if app.is_filtering {
            Style::default().fg(theme.path_bg_search).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.path_fg)
        });
    frame.render_widget(path_bar, outer[0]);

    // ── Sidebar (Drives) ───────────────────────────────────────────────────
    if app.show_sidebar {
        if let Some(d_area) = drives_area_opt {
            let sidebar_items: Vec<ListItem> = app.drives
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let row_style = if i == app.drives_cursor && app.active_pane == Pane::Sidebar {
                        Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD)
                    } else if i == app.drives_cursor {
                        Style::default().bg(Color::DarkGray)
                    } else {
                        Style::default()
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(format!(" 💿 {}", p.name), row_style),
                    ]))
                })
                .collect();
                
            let list = List::new(sidebar_items)
                .block(pane_border(" Disks ", app.active_pane == Pane::Sidebar, &theme))
                .highlight_style(Style::default().bg(theme.selection_bg).add_modifier(Modifier::BOLD));
            frame.render_stateful_widget(list, d_area, &mut app.drives_state);
        }
    }

    // ── Projects list ────────────────────────────────────────────────────────
    let list_items: Vec<ListItem> = if app.projects.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No folders found",
            Style::default().fg(Color::DarkGray),
        )))]
    } else {
        app.projects
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let is_nav = p.name == "..";
                
                let checkbox = if is_nav {
                    "   "
                } else if p.selected {
                    "[✓]"
                } else {
                    "[ ]"
                };
                let check_style = if p.selected {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                
                let row_style = if i == app.cursor && app.active_pane == Pane::Projects {
                    Style::default().bg(theme.selection_bg).add_modifier(Modifier::BOLD)
                } else if i == app.cursor {
                    Style::default().bg(theme.selection_bg)
                } else {
                    Style::default()
                };
                
                let status = app.statuses.get(&p.name).unwrap_or(&ProcessStatus::Idle);
                let url_span = match app.urls.get(&p.name) {
                    Some(url) => Span::styled(format!("  🌐 {}", url), Style::default().fg(theme.url_fg)),
                    None      => Span::raw(""),
                };
                let health_span = if *status == ProcessStatus::Running {
                    match app.health.get(&p.name) {
                        Some(true)  => Span::styled(" ●", Style::default().fg(theme.status_running)),
                        Some(false) => Span::styled(" ●", Style::default().fg(theme.status_error)),
                        None        => Span::styled(" ○", Style::default().fg(Color::DarkGray)),
                    }
                } else {
                    Span::raw("")
                };
                let metrics_span = if *status == ProcessStatus::Running {
                    match app.metrics.get(&p.name) {
                        Some((cpu, mem)) => {
                            let mem_mb = mem / 1_048_576;
                            Span::styled(
                                format!("  {:.1}% {}MB", cpu, mem_mb),
                                Style::default().fg(Color::Magenta),
                            )
                        }
                        None => Span::raw(""),
                    }
                } else {
                    Span::raw("")
                };
                let status_span = if is_nav {
                    Span::raw("")
                } else {
                    Span::styled(status.label(), Style::default().fg(status.color(&theme)))
                };

                let name_width = if projects_area.width < 50 { 15 } else if projects_area.width < 80 { 20 } else { 30 };
                
                let mut row = vec![
                    Span::styled(format!(" {} ", checkbox), check_style),
                ];
                
                if projects_area.width > 40 {
                    row.push(Span::styled(
                        format!("[{}] ", p.project_type.tag()),
                        Style::default().fg(p.project_type.color()),
                    ));
                }
                
                row.push(Span::styled(format!("{:<width$}", p.name, width = name_width), row_style));
                
                if projects_area.width > 30 {
                   row.push(status_span);
                }
                
                if projects_area.width > 60 {
                    row.push(health_span);
                }
                
                if projects_area.width > 80 {
                    row.push(url_span);
                }
                
                if projects_area.width > 110 {
                    row.push(metrics_span);
                }

                ListItem::new(Line::from(row))
            })
            .collect()
    };

    let list = List::new(list_items)
        .block(pane_border(" Projects ", app.active_pane == Pane::Projects, &theme))
        .highlight_style(Style::default().bg(theme.selection_bg).add_modifier(Modifier::BOLD));
    frame.render_stateful_widget(list, projects_area, &mut app.list_state);

    // ── Log panel (color-coded by level) ─────────────────────────────────────
    if let Some(l_area) = log_area_opt {
        let log_capacity = (l_area.height.saturating_sub(2)) as usize;
        
        let filtered_logs: Vec<&LogEntry> = app.log.iter()
            .filter(|e| {
                if let Some(filter) = &app.log_filter {
                    &e.project == filter || e.project == "System"
                } else {
                    true
                }
            })
            .collect();
            
        let log_lines: Vec<Line> = filtered_logs
            .into_iter()
            .rev()
            .take(log_capacity.max(1))
            .map(|entry| {
                let msg = &entry.message;
                let color = if msg.contains("ERROR") || msg.contains("error") || msg.contains("✗") {
                    theme.status_error
                } else if msg.contains("WARN") || msg.contains("warn") || msg.contains("⚠") {
                    Color::Yellow
                } else if msg.contains("🌐") {
                    theme.url_fg
                } else {
                    Color::White
                };
                
                let proj_color = if entry.project == "System" { Color::DarkGray } else { Color::Cyan };
                
                Line::from(vec![
                    Span::styled(format!("[{}] ", entry.timestamp), Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("{:>12} | ", entry.project), Style::default().fg(proj_color)),
                    Span::styled(msg.as_str(), Style::default().fg(color)),
                ])
            })
            .collect();
            
        let log_title = match &app.log_filter {
            Some(p) => format!(" Log ({}) ", p),
            None => " Log (General) ".to_string(),
        };
        let log_widget = Paragraph::new(log_lines)
            .block(pane_border(&log_title, app.active_pane == Pane::Logs, &theme));
        frame.render_widget(log_widget, l_area);
    }

    // ── Running Services panel ─────────────────────────────────────
    if let Some(r_area) = running_area_opt {
        let running_items: Vec<ListItem> = {
            let mut items: Vec<ListItem> = app.statuses
                .iter()
                .filter(|(_, s)| **s == ProcessStatus::Running)
                .map(|(name, _)| {
                    // Try to find the project for type/tag info
                    let proj = app.all_projects.iter().find(|p| &p.name == name);
                    let type_span = match proj {
                        Some(p) => Span::styled(
                            format!("[{}] ", p.project_type.tag()),
                            Style::default().fg(p.project_type.color()),
                        ),
                        None => Span::raw(""),
                    };
                    let health_icon = match app.health.get(name) {
                        Some(true)  => Span::styled(" ● ", Style::default().fg(theme.status_running)),
                        Some(false) => Span::styled(" ● ", Style::default().fg(theme.status_error)),
                        None        => Span::styled(" ○ ", Style::default().fg(Color::DarkGray)),
                    };
                    let url_span = match app.urls.get(name) {
                        Some(url) => Span::styled(format!("  🌐 {}", url), Style::default().fg(theme.url_fg)),
                        None      => Span::raw(""),
                    };
                    let metrics_span = match app.metrics.get(name) {
                        Some((cpu, mem)) if r_area.width > 90 => {
                            let mem_mb = mem / 1_048_576;
                            Span::styled(
                                format!("  {:.1}% {}MB", cpu, mem_mb),
                                Style::default().fg(Color::Magenta),
                            )
                        }
                        _ => Span::raw(""),
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(" ▶ ", Style::default().fg(theme.status_running)),
                        type_span,
                        Span::styled(name.clone(), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                        health_icon,
                        url_span,
                        metrics_span,
                    ]))
                })
                .collect();
            if items.is_empty() {
                items.push(ListItem::new(Line::from(Span::styled(
                    "  No services running",
                    Style::default().fg(Color::DarkGray),
                ))));
            }
            items
        };

        let running_count = app.statuses.values().filter(|s| **s == ProcessStatus::Running).count();
        let panel_title = format!(" ▶ Running ({}) ", running_count);
        let running_list = List::new(running_items)
            .block(pane_border(&panel_title, app.active_pane == Pane::Running, &theme));
        frame.render_widget(running_list, r_area);
    }

    // ── Controls bar ─────────────────────────────────────────────────────────
    let controls = Paragraph::new(footer_lines)
    .block(Block::default().borders(Borders::ALL).title(" Controls ").border_style(Style::default().fg(theme.inactive_border)));
    frame.render_widget(controls, footer_area);

    // ── Install confirmation popup ────────────────────────────────────────────
    if let Some(req) = &app.pending_install {
        use ratatui::{
            layout::Rect,
            widgets::Clear,
        };

        let popup_w = (area.width / 2).max(50).min(area.width.saturating_sub(4));
        let popup_h = 7u16;
        let popup_x = area.x + (area.width.saturating_sub(popup_w)) / 2;
        let popup_y = area.y + (area.height.saturating_sub(popup_h)) / 2;
        let popup_area = Rect::new(popup_x, popup_y, popup_w, popup_h);

        frame.render_widget(Clear, popup_area);

        let msg = format!(" {}", req.description);
        let content = vec![
            Line::from(Span::raw("")),
            Line::from(Span::styled(&msg, Style::default().fg(Color::White).add_modifier(Modifier::BOLD))),
            Line::from(Span::raw("")),
            Line::from(vec![
                Span::styled("  [Y] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::styled("Yes, install ", Style::default().fg(Color::White)),
                Span::styled("   [N] ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                Span::styled("No, skip", Style::default().fg(Color::White)),
            ]),
        ];

        let popup_block = Block::default()
            .title(" 📦 Install Dependencies? ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
            .style(Style::default().bg(Color::Rgb(30, 30, 40)));

        frame.render_widget(Paragraph::new(content).block(popup_block), popup_area);
    }
}

// ── Event loop ───────────────────────────────────────────────────────────────

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    bg_rx: &mut mpsc::UnboundedReceiver<BgMsg>,
) -> Result<()> {
    let (key_tx, mut key_rx) = mpsc::unbounded_channel::<KeyEvent>();
    tokio::task::spawn_blocking(move || {
        loop {
            match event::read() {
                Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                    if key_tx.send(key).is_err() {
                        break;
                    }
                }
                Err(_) => break,
                _ => {}
            }
        }
    });

    let mut redraw_interval = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            _ = redraw_interval.tick() => {
                app.apply_bg_msgs(bg_rx);
                terminal.draw(|f| render(f, app))?;
            }

            Some(key) = key_rx.recv() => {
                let code = key.code;
                // ── Intercept keys for install confirmation popup ────────────
                if app.pending_install.is_some() {
                    match code {
                        KeyCode::Char('y') | KeyCode::Char('Y') => {
                            if let Some(req) = app.pending_install.take() {
                                let _ = req.confirm_tx.send(true);
                            }
                        }
                        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                            if let Some(req) = app.pending_install.take() {
                                let _ = req.confirm_tx.send(false);
                            }
                        }
                        _ => {}
                    }
                    app.apply_bg_msgs(bg_rx);
                    terminal.draw(|f| render(f, app))?;
                    continue;
                }

                if app.is_filtering {
                    match code {
                        KeyCode::Char(c) => {
                            app.filter.push(c);
                            app.apply_filter();
                        }
                        KeyCode::Backspace => {
                            app.filter.pop();
                            app.apply_filter();
                        }
                        KeyCode::Esc => {
                            app.is_filtering = false;
                            app.filter.clear();
                            app.apply_filter();
                        }
                        KeyCode::Enter => {
                            app.is_filtering = false;
                        }
                        _ => {}
                    }
                    continue;
                }


                let kb = &app.config.keybindings;

                // Arrow keys: move the focused panel (only if no modifiers, to prevent clash with expand_left alt-left)
                if key.modifiers.is_empty() && matches!(code, KeyCode::Left | KeyCode::Right | KeyCode::Up | KeyCode::Down) {
                    app.move_active_panel(code);
                    app.apply_bg_msgs(bg_rx);
                    terminal.draw(|f| render(f, app))?;
                    continue;
                }

                if matches_key(&key, &kb.up) { app.move_up(); }
                else if matches_key(&key, &kb.down) { app.move_down(); }
                else if matches_key(&key, &kb.enter) { app.enter_item(); }
                else if matches_key(&key, &kb.back) { app.go_back(); }
                else if code == KeyCode::Char(' ') { app.toggle_selection(); }
                else if matches_key(&key, &kb.run) { app.launch_selected(); }
                else if matches_key(&key, &kb.stop) { app.kill_selected(); }
                else if code == KeyCode::Char('l') || code == KeyCode::Char('L') { app.layout_active_panel(); }
                else if code == KeyCode::Char('h') || code == KeyCode::Char('H') { app.hide_active_panel(); }
                else if matches_key(&key, &kb.toggle_split) { app.toggle_split_direction(); }
                else if matches_key(&key, &kb.next_pane) { app.next_pane(); }
                else if matches_key(&key, &kb.prev_pane) { app.prev_pane(); }
                else if matches_key(&key, &kb.expand_left) { app.resize_active_panel(-2, 0); }
                else if matches_key(&key, &kb.expand_right) { app.resize_active_panel(2, 0); }
                else if matches_key(&key, &kb.expand_up) { app.resize_active_panel(0, -2); }
                else if matches_key(&key, &kb.expand_down) { app.resize_active_panel(0, 2); }
                else if code == KeyCode::Char('/') {
                    app.is_filtering = true;
                    app.active_pane = Pane::Projects;
                }
                else if matches_key(&key, &kb.settings) { app.open_settings(); }
                else if matches_key(&key, &kb.clear_log) { app.clear_log(); }
                else if matches_key(&key, &kb.cycle_log_view) { app.cycle_log_view(); }
                else if matches_key(&key, &kb.export_log) { app.export_current_log(); }
                else if matches_key(&key, &kb.quit) {
                    app.save_layout();
                    break;
                }

                app.apply_bg_msgs(bg_rx);
                terminal.draw(|f| render(f, app))?;
            }
        }
    }
    Ok(())
}

fn matches_key(key: &KeyEvent, target: &str) -> bool {
    let target = target.to_lowercase();
    let mut parts: Vec<&str> = target.split('-').collect();
    let key_name = parts.pop().unwrap_or("").trim();
    
    let mut req_alt = false;
    let mut req_ctrl = false;
    
    for p in parts {
        match p.trim() {
            "alt" => req_alt = true,
            "ctrl" => req_ctrl = true,
            _ => {}
        }
    }
    
    let has_alt = key.modifiers.contains(KeyModifiers::ALT);
    let has_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    
    if req_alt != has_alt || req_ctrl != has_ctrl {
        return false;
    }
    
    match key.code {
        KeyCode::Char(c) => c.to_lowercase().to_string() == key_name,
        KeyCode::Tab => key_name == "tab",
        KeyCode::BackTab => key_name == "backtab" || key_name == "shift-tab",
        KeyCode::Enter => key_name == "enter",
        KeyCode::Up => key_name == "up",
        KeyCode::Down => key_name == "down",
        KeyCode::Left => key_name == "left",
        KeyCode::Right => key_name == "right",
        KeyCode::Esc => key_name == "esc",
        KeyCode::Backspace => key_name == "backspace",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parsing() {
        let toml_str = r#"
            [general]
            exclude_drives = ["A:\\", "B:\\"]
            [keybindings]
            up = "k"
            down = "j"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.general.exclude_drives, vec!["A:\\", "B:\\"]);
        assert_eq!(config.keybindings.up, "k");
        assert_eq!(config.keybindings.down, "j");
        assert_eq!(config.keybindings.quit, "q"); // Default value
    }

    #[test]
    fn test_drive_filtering() {
        // This is tricky to test cross-platform, but we can test the logic
        let exclude = vec!["C:\\".to_string()];
        
        // Mocking the behavior for testing purposes if needed
        // Since get_drives uses winapi on windows, we can only truly test it there
        #[cfg(windows)]
        {
            let drives = get_drives(&exclude);
            for d in drives {
                assert_ne!(d.to_str().unwrap(), "C:\\");
            }
        }
    }
}

// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let scan_dir = std::env::current_dir()
        .unwrap_or_else(|_| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")));

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let (bg_tx, mut bg_rx) = mpsc::unbounded_channel::<BgMsg>();
    let (config, config_path) = Config::load();
    let mut app = App::new(scan_dir, config, config_path, bg_tx);
    let result = run_app(&mut terminal, &mut app, &mut bg_rx).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}
