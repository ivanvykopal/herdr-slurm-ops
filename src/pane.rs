//! The interactive ops pane. One UI thread owns all state and rendering;
//! every network call (queries, actions, detail, log follow, agent list)
//! runs on a worker thread and reports back through a message channel, so a
//! slow login node never delays a keypress.

use super::*;
use crate::bindings::*;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use std::collections::{HashSet, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::process::Child;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;

const MAX_LOG_LINES: usize = 5000;

type JobKey = (String, String); // (machine name, job id)
type Snapshot = Result<(Vec<Machine>, HashMap<String, QueryResult>), String>;

const HELP: &[&str] = &[
    "Scheduler Ops - keys",
    "",
    "j/k Up/Down  move      g/G  first/last",
    "Space        mark job (h u e x t then",
    "             act on all marked jobs)",
    "Enter        job detail (scrollable)",
    "o            follow the job's log",
    "n            notify an agent when the",
    "             job ends (n again detaches)",
    "h / u        hold / release",
    "e            requeue (press twice)",
    "x            cancel (press twice)",
    "t            set time limit, e.g.",
    "             2:00:00 or 1-12:00:00",
    "/            filter id/name/state/node",
    "s            sort queue/state/name/time",
    "a            all users    H  24h history",
    "r            refresh now",
    "Esc          clear marks, then filter",
    "q            quit",
    "",
    "Detail/log/help: j/k PgUp/PgDn g/G",
    "scroll, q/Esc back.",
    "'+' marked job, '*' agent will be told.",
];

// ------------------------------------------------------------------ helpers --

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    Queue,
    State,
    Name,
    Elapsed,
}

impl Sort {
    fn next(self) -> Sort {
        match self {
            Sort::Queue => Sort::State,
            Sort::State => Sort::Name,
            Sort::Name => Sort::Elapsed,
            Sort::Elapsed => Sort::Queue,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Sort::Queue => "queue",
            Sort::State => "state",
            Sort::Name => "name",
            Sort::Elapsed => "elapsed",
        }
    }
}

/// `d-hh:mm:ss`, `hh:mm:ss`, `mm:ss` -> seconds (unparseable parts = 0).
fn elapsed_secs(s: &str) -> u64 {
    let (days, rest) = match s.split_once('-') {
        Some((d, r)) => (d.parse::<u64>().unwrap_or(0), r),
        None => (0, s),
    };
    let secs = rest
        .split(':')
        .fold(0u64, |acc, p| acc * 60 + p.parse::<u64>().unwrap_or(0));
    days * 86_400 + secs
}

fn state_rank(state: &str) -> u8 {
    match state {
        "RUNNING" => 0,
        "PENDING" => 1,
        "COMPLETING" | "SUSPENDED" => 2,
        _ => 3,
    }
}

fn sort_jobs(jobs: &mut [&Job], sort: Sort) {
    match sort {
        Sort::Queue => {}
        Sort::State => jobs.sort_by_key(|j| state_rank(&j.state)),
        Sort::Name => jobs.sort_by_key(|j| j.name.to_lowercase()),
        Sort::Elapsed => jobs.sort_by_key(|j| std::cmp::Reverse(elapsed_secs(&j.elapsed))),
    }
}

fn matches_filter(job: &Job, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }
    let needle = filter.to_lowercase();
    [&job.id, &job.name, &job.state, &job.account, &job.where_]
        .iter()
        .any(|f| f.to_lowercase().contains(&needle))
}

/// Remove ANSI escapes and control characters from one remote output line
/// (keeping only the text after the last carriage return, i.e. the final
/// state of a progress bar) so it cannot corrupt the pane.
fn clean_line(raw: &str) -> String {
    let trimmed = raw.trim_end_matches(['\n', '\r']);
    let seg = trimmed.rsplit('\r').next().unwrap_or(trimmed);
    let mut out = String::with_capacity(seg.len());
    let mut chars = seg.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\x1b' => match chars.peek().copied() {
                Some('[') => {
                    chars.next();
                    for n in chars.by_ref() {
                        if ('@'..='~').contains(&n) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next();
                    while let Some(n) = chars.next() {
                        if n == '\x07' {
                            break;
                        }
                        if n == '\x1b' {
                            chars.next();
                            break;
                        }
                    }
                }
                Some(_) => {
                    chars.next();
                }
                None => {}
            },
            '\t' => out.push_str("    "),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

/// Hard-wrap to `w` columns (char count); an empty line stays one line.
fn wrap(line: &str, w: usize) -> Vec<String> {
    if line.is_empty() {
        return vec![String::new()];
    }
    let chars: Vec<char> = line.chars().collect();
    chars
        .chunks(w.max(1))
        .map(|c| c.iter().collect())
        .collect()
}

fn term_size() -> (usize, usize) {
    crossterm::terminal::size()
        .map(|(w, h)| ((w as usize).max(20), (h as usize).max(6)))
        .unwrap_or((80, 24))
}

fn local_clock(t: SystemTime) -> String {
    local_timestamp(&t)
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .to_string()
}

// -------------------------------------------------------------------- model --

enum Msg {
    Snapshot { scope: Scope, data: Snapshot },
    Status(String),
    Refresh,
    Detail { gen: u64, result: Result<String, String> },
    FollowReady { gen: u64, result: Result<(Child, String), String> },
    Log { gen: u64, err: bool, line: String },
    Agents { target: (String, Job), result: Result<Vec<AgentInfo>, String> },
}

struct Pager {
    title: String,
    lines: VecDeque<(bool, String)>, // (is_stderr, text)
    top: usize,
    bottom: bool, // stick to the newest line (live logs)
    gen: u64,
    job: Option<(usize, Job)>, // detail view: job the action keys apply to
    live: bool,
    child: Option<Child>,
}

impl Drop for Pager {
    fn drop(&mut self) {
        if let Some(c) = self.child.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

enum InputKind {
    Filter,
    TimeLimit(Vec<(usize, Job)>),
}

enum Mode {
    List,
    Pager(Pager),
    Picker {
        target: (String, Job),
        agents: Vec<AgentInfo>,
        idx: usize,
    },
    Input {
        prompt: String,
        buf: String,
        kind: InputKind,
    },
}

struct App {
    machines: Vec<Machine>,
    results: HashMap<String, QueryResult>,
    load_error: Option<String>,
    scope: Scope,
    scope_flag: Arc<AtomicU8>,
    refresh_tx: Sender<()>,
    updated: Option<SystemTime>,
    loading: bool,
    sel: Option<JobKey>,
    cursor_hint: usize,
    marked: HashSet<JobKey>,
    filter: String,
    sort: Sort,
    status: String,
    mode: Mode,
    armed: Option<(String, Vec<JobKey>)>,
    bindings: Vec<Binding>,
    tx: Sender<Msg>,
    next_gen: u64,
    quit: bool,
    list_top: std::cell::Cell<usize>,
}

struct Row {
    mi: usize,
    job: Job,
}

impl App {
    fn key_of(&self, r: &Row) -> JobKey {
        (self.machines[r.mi].name.clone(), r.job.id.clone())
    }

    fn rows(&self) -> Vec<Row> {
        let mut out = Vec::new();
        for (mi, m) in self.machines.iter().enumerate() {
            let Some(Ok(jobs)) = self.results.get(&m.name) else {
                continue;
            };
            let mut group: Vec<&Job> = jobs
                .iter()
                .filter(|j| matches_filter(j, &self.filter))
                .collect();
            sort_jobs(&mut group, self.sort);
            out.extend(group.into_iter().map(|j| Row { mi, job: j.clone() }));
        }
        out
    }

    /// Keep the selection on the same job across refreshes/reorders; when
    /// it disappears, stay near the old position.
    fn sync_cursor(&mut self) {
        let rows = self.rows();
        if rows.is_empty() {
            self.sel = None;
            return;
        }
        let idx = self
            .sel
            .as_ref()
            .and_then(|k| rows.iter().position(|r| self.key_of(r) == *k))
            .unwrap_or_else(|| self.cursor_hint.min(rows.len() - 1));
        self.cursor_hint = idx;
        self.sel = Some(self.key_of(&rows[idx]));
    }

    fn move_cursor(&mut self, delta: isize) {
        let rows = self.rows();
        if rows.is_empty() {
            return;
        }
        let cur = self
            .sel
            .as_ref()
            .and_then(|k| rows.iter().position(|r| self.key_of(r) == *k))
            .unwrap_or(self.cursor_hint.min(rows.len() - 1));
        let new = (cur as isize + delta).clamp(0, rows.len() as isize - 1) as usize;
        self.cursor_hint = new;
        self.sel = Some(self.key_of(&rows[new]));
    }

    fn selected(&self) -> Option<(usize, Job)> {
        let sel = self.sel.as_ref()?;
        self.rows()
            .into_iter()
            .find(|r| self.key_of(r) == *sel)
            .map(|r| (r.mi, r.job))
    }

    /// Jobs an action applies to: the detail view's job, else the marked
    /// jobs, else the selected one.
    fn targets(&self) -> Vec<(usize, Job)> {
        if let Mode::Pager(p) = &self.mode {
            return p.job.clone().into_iter().collect();
        }
        if !self.marked.is_empty() {
            return self
                .rows()
                .into_iter()
                .filter(|r| self.marked.contains(&self.key_of(r)))
                .map(|r| (r.mi, r.job))
                .collect();
        }
        self.selected().into_iter().collect()
    }

    fn request_refresh(&mut self) {
        self.loading = true;
        let _ = self.refresh_tx.send(());
    }

    fn set_scope(&mut self, scope: Scope) {
        self.scope = if self.scope == scope { Scope::Mine } else { scope };
        self.scope_flag.store(self.scope.to_u8(), Ordering::SeqCst);
        self.results.clear();
        self.marked.clear();
        self.sel = None;
        self.cursor_hint = 0;
        self.status.clear();
        self.request_refresh();
    }

    fn can_mutate(&mut self) -> bool {
        if self.scope == Scope::History {
            self.status = "finished jobs (history view): action not available".into();
            return false;
        }
        true
    }

    fn gen(&mut self) -> u64 {
        self.next_gen += 1;
        self.next_gen
    }

    // ---------------------------------------------------------- messages --

    fn on_msg(&mut self, msg: Msg) {
        match msg {
            Msg::Snapshot { scope, data } => {
                if scope != self.scope {
                    return; // stale answer for a scope the user left
                }
                self.loading = false;
                match data {
                    Ok((machines, results)) => {
                        self.machines = machines;
                        self.results = results;
                        self.load_error = None;
                        self.updated = Some(SystemTime::now());
                    }
                    Err(e) => self.load_error = Some(e),
                }
                let live: HashSet<JobKey> = self.rows().iter().map(|r| self.key_of(r)).collect();
                self.marked.retain(|k| live.contains(k));
                self.sync_cursor();
                self.bindings = load_bindings();
            }
            Msg::Status(s) => self.status = s,
            Msg::Refresh => self.request_refresh(),
            Msg::Detail { gen, result } => {
                if let Mode::Pager(p) = &mut self.mode {
                    if p.gen == gen {
                        p.lines = match result {
                            Ok(text) => text.lines().map(|l| (false, clean_line(l))).collect(),
                            Err(e) => VecDeque::from(vec![(true, format!("detail failed: {e}"))]),
                        };
                    }
                }
            }
            Msg::FollowReady { gen, result } => match (&mut self.mode, result) {
                (Mode::Pager(p), Ok((child, path))) if p.gen == gen => {
                    p.title = format!("log {path}");
                    p.child = Some(child);
                }
                (Mode::Pager(p), Err(e)) if p.gen == gen => {
                    p.title = "log".into();
                    p.lines.push_back((true, format!("follow failed: {e}")));
                }
                (_, Ok((mut child, _))) => {
                    // The user left before the log opened: do not leak ssh.
                    let _ = child.kill();
                    let _ = child.wait();
                }
                _ => {}
            },
            Msg::Log { gen, err, line } => {
                if let Mode::Pager(p) = &mut self.mode {
                    if p.gen == gen {
                        p.lines.push_back((err, line));
                        if p.lines.len() > MAX_LOG_LINES {
                            p.lines.pop_front();
                            p.top = p.top.saturating_sub(1);
                        }
                    }
                }
            }
            Msg::Agents { target, result } => {
                if !matches!(self.mode, Mode::List) {
                    return;
                }
                match result {
                    Ok(agents) if agents.is_empty() => self.status = "no agent panes found".into(),
                    Ok(agents) => {
                        self.status.clear();
                        self.mode = Mode::Picker {
                            target,
                            agents,
                            idx: 0,
                        };
                    }
                    Err(e) => self.status = e,
                }
            }
        }
    }

    // -------------------------------------------------------------- keys --

    fn on_key(&mut self, k: KeyEvent) {
        // Messages (action results, armed prompts) last until the next key,
        // so a stale "press x again" can never outlive its armed state.
        if !matches!(self.mode, Mode::Input { .. }) {
            self.status.clear();
        }
        if k.modifiers.contains(KeyModifiers::CONTROL) {
            // Raw mode delivers Ctrl+C as a key: leave sub-views, never
            // kill the pane by accident.
            if k.code == KeyCode::Char('c') && !matches!(self.mode, Mode::List) {
                self.mode = Mode::List;
            }
            return;
        }
        match &self.mode {
            Mode::List => self.on_list_key(k.code),
            Mode::Pager(_) => self.on_pager_key(k.code),
            Mode::Picker { .. } => self.on_picker_key(k.code),
            Mode::Input { .. } => self.on_input_key(k.code),
        }
    }

    fn on_list_key(&mut self, code: KeyCode) {
        let armed = self.armed.take();
        match code {
            KeyCode::Char('q') | KeyCode::Char('Q') => self.quit = true,
            KeyCode::Esc => {
                if !self.marked.is_empty() {
                    self.marked.clear();
                    self.status = "marks cleared".into();
                } else if !self.filter.is_empty() {
                    self.filter.clear();
                    self.sync_cursor();
                    self.status = "filter cleared".into();
                }
            }
            KeyCode::Char('j') | KeyCode::Down => self.move_cursor(1),
            KeyCode::Char('k') | KeyCode::Up => self.move_cursor(-1),
            KeyCode::PageDown => self.move_cursor(10),
            KeyCode::PageUp => self.move_cursor(-10),
            KeyCode::Home | KeyCode::Char('g') => self.move_cursor(isize::MIN / 2),
            KeyCode::End | KeyCode::Char('G') => self.move_cursor(isize::MAX / 2),
            KeyCode::Char(' ') => {
                if let Some(sel) = self.sel.clone() {
                    if !self.marked.remove(&sel) {
                        self.marked.insert(sel);
                    }
                    self.move_cursor(1);
                }
            }
            KeyCode::Enter => {
                if let Some((mi, job)) = self.selected() {
                    self.open_detail(mi, job);
                }
            }
            KeyCode::Char('o') => {
                if let Some((mi, job)) = self.selected() {
                    self.open_follow(mi, job);
                }
            }
            KeyCode::Char('n') => self.bind_toggle(),
            KeyCode::Char('h') => self.simple_action("hold"),
            KeyCode::Char('u') => self.simple_action("release"),
            KeyCode::Char('e') => self.destructive("requeue", 'e', armed),
            KeyCode::Char('x') => self.destructive("cancel", 'x', armed),
            KeyCode::Char('t') => self.ask_time_limit(),
            KeyCode::Char('/') => {
                self.mode = Mode::Input {
                    prompt: "filter: ".into(),
                    buf: self.filter.clone(),
                    kind: InputKind::Filter,
                }
            }
            KeyCode::Char('s') => {
                self.sort = self.sort.next();
                self.sync_cursor();
                self.status = format!("sort: {}", self.sort.label());
            }
            KeyCode::Char('a') => self.set_scope(Scope::All),
            KeyCode::Char('H') => self.set_scope(Scope::History),
            KeyCode::Char('r') | KeyCode::Char('R') => {
                self.status = "refreshing…".into();
                self.request_refresh();
            }
            KeyCode::Char('?') => self.open_help(),
            _ => {}
        }
    }

    fn on_pager_key(&mut self, code: KeyCode) {
        let armed = self.armed.take();
        let (live, has_job) = match &self.mode {
            Mode::Pager(p) => (p.live, p.job.is_some()),
            _ => return,
        };
        match code {
            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                self.mode = Mode::List;
                return;
            }
            KeyCode::Enter if !live => {
                self.mode = Mode::List;
                return;
            }
            KeyCode::Char('j') | KeyCode::Down => self.scroll(1),
            KeyCode::Char('k') | KeyCode::Up => self.scroll(-1),
            KeyCode::PageDown | KeyCode::Char(' ') => self.scroll(self.page() as isize),
            KeyCode::PageUp | KeyCode::Char('b') => self.scroll(-(self.page() as isize)),
            KeyCode::Home | KeyCode::Char('g') => {
                if let Mode::Pager(p) = &mut self.mode {
                    p.top = 0;
                    p.bottom = false;
                }
            }
            KeyCode::End | KeyCode::Char('G') => {
                if let Mode::Pager(p) = &mut self.mode {
                    p.bottom = true;
                }
            }
            _ => {}
        }
        if live || !has_job {
            return;
        }
        match code {
            KeyCode::Char('o') => {
                if let Some((mi, job)) = self.targets().into_iter().next() {
                    self.open_follow(mi, job);
                }
            }
            KeyCode::Char('n') => {
                // The picker works from the list: return to it first.
                let t = self.targets().into_iter().next();
                self.mode = Mode::List;
                self.sel = t.map(|(mi, j)| (self.machines[mi].name.clone(), j.id));
                self.bind_toggle();
            }
            KeyCode::Char('h') => self.simple_action("hold"),
            KeyCode::Char('u') => self.simple_action("release"),
            KeyCode::Char('e') => self.destructive("requeue", 'e', armed),
            KeyCode::Char('x') => self.destructive("cancel", 'x', armed),
            KeyCode::Char('t') => self.ask_time_limit(),
            _ => {}
        }
    }

    fn page(&self) -> usize {
        term_size().1.saturating_sub(3).max(1)
    }

    fn scroll(&mut self, delta: isize) {
        let (w, h) = term_size();
        let Mode::Pager(p) = &mut self.mode else {
            return;
        };
        let total: usize = p.lines.iter().map(|(_, l)| wrap(l, w).len()).sum();
        let max_top = total.saturating_sub(h.saturating_sub(2));
        let cur = if p.bottom { max_top } else { p.top.min(max_top) };
        let new = (cur as isize + delta).clamp(0, max_top as isize) as usize;
        p.top = new;
        p.bottom = p.live && new == max_top;
    }

    fn on_picker_key(&mut self, code: KeyCode) {
        let Mode::Picker { target, agents, idx } = &mut self.mode else {
            return;
        };
        let chosen = match code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.mode = Mode::List;
                self.status = "cancelled".into();
                return;
            }
            KeyCode::Char('j') | KeyCode::Down => {
                *idx = (*idx + 1).min(agents.len() - 1);
                return;
            }
            KeyCode::Char('k') | KeyCode::Up => {
                *idx = idx.saturating_sub(1);
                return;
            }
            KeyCode::Enter => *idx,
            KeyCode::Char(c) => match c.to_digit(10) {
                Some(d) if d >= 1 && (d as usize) <= agents.len().min(9) => d as usize - 1,
                _ => return,
            },
            _ => return,
        };
        let (target, agent) = (target.clone(), agents[chosen].clone());
        self.mode = Mode::List;
        let key = binding_key(&target.1.id);
        let added = update_bindings(|bs| {
            if bs.iter().any(|b| binding_matches(b, &target.0, &key)) {
                return false;
            }
            bs.push(Binding {
                machine: target.0.clone(),
                job_id: key.clone(),
                pane_id: agent.pane.clone(),
                agent: agent.agent.clone(),
            });
            true
        });
        self.bindings = load_bindings();
        self.status = if added {
            format!(
                "job {key}: {} [{}] will be notified",
                agent.agent, agent.pane
            )
        } else {
            "job already bound".into()
        };
    }

    fn on_input_key(&mut self, code: KeyCode) {
        let Mode::Input { buf, .. } = &mut self.mode else {
            return;
        };
        match code {
            KeyCode::Esc => self.mode = Mode::List,
            KeyCode::Backspace => {
                buf.pop();
            }
            KeyCode::Char(c) => buf.push(c),
            KeyCode::Enter => {
                let Mode::Input { buf, kind, .. } = std::mem::replace(&mut self.mode, Mode::List)
                else {
                    return;
                };
                match kind {
                    InputKind::Filter => {
                        self.filter = buf.trim().to_string();
                        self.marked.clear(); // never act on rows the filter hides
                        self.sync_cursor();
                    }
                    InputKind::TimeLimit(targets) => {
                        let value = buf.trim().to_string();
                        if valid_time_limit(&value) {
                            self.start_bulk("timelimit", targets, Some(value));
                        } else {
                            self.status = "invalid time limit".into();
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // ----------------------------------------------------------- actions --

    fn simple_action(&mut self, verb: &'static str) {
        if !self.can_mutate() {
            return;
        }
        let targets = self.targets();
        if !targets.is_empty() {
            self.start_bulk(verb, targets, None);
        }
    }

    /// Two-step: the first press arms and names the jobs, an identical
    /// second press on the identical job set runs it. Any other key disarms.
    fn destructive(
        &mut self,
        verb: &'static str,
        key: char,
        armed: Option<(String, Vec<JobKey>)>,
    ) {
        if !self.can_mutate() {
            return;
        }
        let targets = self.targets();
        if targets.is_empty() {
            return;
        }
        let keys: Vec<JobKey> = targets
            .iter()
            .map(|(mi, j)| (self.machines[*mi].name.clone(), j.id.clone()))
            .collect();
        if armed.is_some_and(|(v, k)| v == verb && k == keys) {
            self.start_bulk(verb, targets, None);
            if verb == "cancel" {
                self.marked.clear();
            }
            return;
        }
        let ids: Vec<&str> = targets.iter().take(3).map(|(_, j)| j.id.as_str()).collect();
        self.status = format!(
            "press {key} again to {} {} job(s): {}{}",
            verb.to_uppercase(),
            targets.len(),
            ids.join(","),
            if targets.len() > 3 { ",…" } else { "" }
        );
        self.armed = Some((verb.to_string(), keys));
    }

    fn start_bulk(&mut self, verb: &'static str, targets: Vec<(usize, Job)>, arg: Option<String>) {
        let list: Vec<(Machine, Job)> = targets
            .into_iter()
            .map(|(mi, j)| (self.machines[mi].clone(), j))
            .collect();
        self.status = format!("{verb}: working on {} job(s)…", list.len());
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(Msg::Status(bulk_action(verb, &list, arg.as_deref())));
            let _ = tx.send(Msg::Refresh);
        });
    }

    fn ask_time_limit(&mut self) {
        if !self.can_mutate() {
            return;
        }
        let targets = self.targets();
        if targets.is_empty() {
            return;
        }
        self.mode = Mode::Input {
            prompt: format!(
                "new time limit for {} job(s) (e.g. 2:00:00, 1-12:00:00): ",
                targets.len()
            ),
            buf: String::new(),
            kind: InputKind::TimeLimit(targets),
        };
    }

    fn bind_toggle(&mut self) {
        if self.scope == Scope::History {
            self.status = "job already finished".into();
            return;
        }
        let Some((mi, job)) = self.selected() else {
            return;
        };
        let machine = self.machines[mi].name.clone();
        let key = binding_key(&job.id);
        let detached = update_bindings(|bs| {
            let before = bs.len();
            bs.retain(|b| !binding_matches(b, &machine, &key));
            bs.len() != before
        });
        self.bindings = load_bindings();
        if detached {
            self.status = format!("detached notification from job {key}");
            return;
        }
        self.status = "loading agents…".into();
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(Msg::Agents {
                target: (machine, job),
                result: list_agents(),
            });
        });
    }

    // -------------------------------------------------------------- views --

    fn new_pager(&mut self, title: String, live: bool, job: Option<(usize, Job)>) -> u64 {
        let gen = self.gen();
        self.mode = Mode::Pager(Pager {
            title,
            lines: VecDeque::new(),
            top: 0,
            bottom: live,
            gen,
            job,
            live,
            child: None,
        });
        gen
    }

    fn open_help(&mut self) {
        self.new_pager("Help".into(), false, None);
        if let Mode::Pager(p) = &mut self.mode {
            p.lines = HELP.iter().map(|l| (false, l.to_string())).collect();
        }
    }

    fn open_detail(&mut self, mi: usize, job: Job) {
        let m = self.machines[mi].clone();
        let finished = self.scope == Scope::History;
        let gen = self.new_pager(
            format!("{} · job {} ({})", m.name, job.id, job.name),
            false,
            Some((mi, job.clone())),
        );
        if let Mode::Pager(p) = &mut self.mode {
            p.lines.push_back((false, "loading…".into()));
        }
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let result = job_detail_remote(&m, &job.id, finished).map(|mut text| {
                if let Ok(sinfo) = sinfo_summary_remote(&m) {
                    if !sinfo.trim().is_empty() {
                        text.push_str("\npartitions (alloc/idle · state):\n");
                        text.push_str(&sinfo);
                    }
                }
                text
            });
            let _ = tx.send(Msg::Detail { gen, result });
        });
    }

    fn open_follow(&mut self, mi: usize, job: Job) {
        let m = self.machines[mi].clone();
        let finished = self.scope == Scope::History;
        let gen = self.new_pager(format!("log · job {} (resolving path…)", job.id), true, None);
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let result = start_follow(&m, &job, finished, gen, &tx);
            let _ = tx.send(Msg::FollowReady { gen, result });
        });
    }

    // ------------------------------------------------------------ drawing --

    fn draw(&self) -> String {
        let (w, h) = term_size();
        let mut lines = match &self.mode {
            Mode::Pager(p) => self.draw_pager(p, w, h),
            Mode::Picker { target, agents, idx } => self.draw_picker(target, agents, *idx, w, h),
            _ => self.draw_list(w, h),
        };
        lines.truncate(h);
        while lines.len() < h {
            lines.push(String::new());
        }
        let mut out = String::from("\x1b[H");
        for (i, l) in lines.iter().enumerate() {
            out.push_str(l);
            out.push_str("\x1b[K");
            if i + 1 < h {
                out.push_str("\r\n");
            }
        }
        out.push_str("\x1b[J");
        out
    }

    fn title_bar(&self, text: &str, w: usize) -> String {
        format!("\x1b[1;7m{}\x1b[0m", pad(&truncate(text, w), w))
    }

    fn bottom_line(&self, w: usize) -> String {
        if let Mode::Input { prompt, buf, .. } = &self.mode {
            let text = format!("{prompt}{buf}");
            let shown: String = text
                .chars()
                .rev()
                .take(w.saturating_sub(1))
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            return format!("\x1b[1m{shown}\x1b[0m█");
        }
        if !self.status.is_empty() {
            return format!("\x1b[1;33m{}\x1b[0m", truncate(&self.status, w));
        }
        format!(
            "\x1b[90m{}\x1b[0m",
            truncate("j/k move  Enter detail  o log  n notify  x cancel  ? help", w)
        )
    }

    fn draw_list(&self, w: usize, h: usize) -> Vec<String> {
        let mut out = vec![self.title_bar(
            &format!(" Scheduler Ops · {} ", self.scope.label()),
            w,
        )];
        let mut summary = match (self.loading, self.updated) {
            (true, _) => "loading…".to_string(),
            (false, Some(t)) => format!("updated {}", local_clock(t)),
            (false, None) => "no data yet".to_string(),
        };
        summary.push_str(&format!(" · sort:{}", self.sort.label()));
        if !self.filter.is_empty() {
            summary.push_str(&format!(" · /{}/", self.filter));
        }
        if !self.marked.is_empty() {
            summary.push_str(&format!(" · {} marked", self.marked.len()));
        }
        out.push(format!("\x1b[90m{}\x1b[0m", truncate(&summary, w)));

        let rows = self.rows();
        let sel_idx = self
            .sel
            .as_ref()
            .and_then(|k| rows.iter().position(|r| self.key_of(r) == *k));
        let mut body: Vec<String> = Vec::new();
        let mut sel_line = 0usize;
        if let Some(e) = &self.load_error {
            for l in wrap(e, w.saturating_sub(2)) {
                body.push(format!("\x1b[31m {l}\x1b[0m"));
            }
        }
        let mut ri = 0usize;
        for (mi, m) in self.machines.iter().enumerate() {
            body.push(format!(
                "\x1b[1m{}\x1b[0m",
                truncate(
                    &format!("▸ {} ({} on {})", m.name, m.scheduler_user, m.ssh_target()),
                    w
                )
            ));
            match self.results.get(&m.name) {
                Some(Err(e)) => {
                    body.push(format!("\x1b[31m{}\x1b[0m", truncate(&format!("  ✗ {e}"), w)));
                    continue;
                }
                None => {
                    body.push("\x1b[90m  loading…\x1b[0m".into());
                    continue;
                }
                Some(Ok(jobs)) if jobs.is_empty() => {
                    body.push("\x1b[90m  (no jobs)\x1b[0m".into());
                    continue;
                }
                Some(Ok(_)) => {}
            }
            while ri < rows.len() && rows[ri].mi == mi {
                let r = &rows[ri];
                let selected = sel_idx == Some(ri);
                if selected {
                    sel_line = body.len();
                }
                body.push(self.row_line(m, &r.job, selected, w));
                ri += 1;
            }
        }
        if rows.is_empty() && self.load_error.is_none() && !self.loading && !self.filter.is_empty() {
            body.push("\x1b[90m  (no matching jobs)\x1b[0m".into());
        }

        let ch = h.saturating_sub(3).max(1);
        let mut top = self.list_top.get().min(body.len().saturating_sub(1));
        if sel_line < top {
            top = sel_line;
        }
        if sel_line >= top + ch {
            top = sel_line + 1 - ch;
        }
        self.list_top.set(top);
        out.extend(body.into_iter().skip(top).take(ch));
        while out.len() < h - 1 {
            out.push(String::new());
        }
        out.push(self.bottom_line(w));
        out
    }

    fn row_line(&self, m: &Machine, j: &Job, selected: bool, w: usize) -> String {
        let marked = self.marked.contains(&(m.name.clone(), j.id.clone()));
        let bound = self.bindings.iter().any(|b| binding_covers(b, &m.name, &j.id));
        let mark = if marked { '+' } else { ' ' };
        let bind = if bound { '*' } else { ' ' };
        let id = pad(&truncate(&j.id, 10), 10);
        let state = pad(&truncate(&j.state, 11), 11);
        let el = pad_left(&truncate(&j.elapsed, 11), 11);
        let name_w = w.saturating_sub(38);
        let plain = format!("{mark}{bind} {id} {state} {el} {}", truncate(&j.name, name_w));
        if selected {
            return format!("\x1b[7m{}\x1b[0m", pad(&truncate(&plain, w), w));
        }
        if w < 38 {
            return truncate(&plain, w);
        }
        format!(
            "{mark}\x1b[1;36m{bind}\x1b[0m {id} {}{state}\x1b[0m {el} {}",
            state_col(&j.state),
            truncate(&j.name, name_w)
        )
    }

    fn draw_picker(
        &self,
        target: &(String, Job),
        agents: &[AgentInfo],
        idx: usize,
        w: usize,
        h: usize,
    ) -> Vec<String> {
        let mut out = vec![self.title_bar(" Notify agent ", w)];
        out.push(format!(
            "\x1b[90m{}\x1b[0m",
            truncate(
                &format!(
                    "job {} · j/k 1-9 pick · Enter ok · Esc",
                    target.1.id
                ),
                w
            )
        ));
        for (i, a) in agents.iter().enumerate().take(h.saturating_sub(4)) {
            let text = truncate(
                &format!(
                    "{} {}. {} — {} [{}]",
                    if i == idx { '>' } else { ' ' },
                    i + 1,
                    a.agent,
                    a.title,
                    a.pane
                ),
                w,
            );
            out.push(if i == idx {
                format!("\x1b[7m{}\x1b[0m", pad(&text, w))
            } else {
                text
            });
        }
        out
    }

    fn draw_pager(&self, p: &Pager, w: usize, h: usize) -> Vec<String> {
        let mut out = vec![self.title_bar(&format!(" {} ", p.title), w)];
        let visual: Vec<(bool, String)> = p
            .lines
            .iter()
            .flat_map(|(err, l)| wrap(l, w).into_iter().map(move |c| (*err, c)))
            .collect();
        let ch = h.saturating_sub(2).max(1);
        let max_top = visual.len().saturating_sub(ch);
        let top = if p.bottom { max_top } else { p.top.min(max_top) };
        for (err, l) in visual.iter().skip(top).take(ch) {
            out.push(if *err {
                format!("\x1b[31m{l}\x1b[0m")
            } else {
                l.clone()
            });
        }
        while out.len() < h - 1 {
            out.push(String::new());
        }
        let hint = if p.live {
            format!(
                "{} · j/k PgUp/PgDn g/G scroll · q back",
                if p.bottom { "following" } else { "paused (G resumes)" }
            )
        } else if p.job.is_some() {
            "j/k PgUp/PgDn g/G scroll · o log n notify x cancel · q back".into()
        } else {
            "j/k PgUp/PgDn g/G scroll · q back".into()
        };
        out.push(if self.status.is_empty() {
            format!("\x1b[90m{}\x1b[0m", truncate(&hint, w))
        } else {
            format!("\x1b[1;33m{}\x1b[0m", truncate(&self.status, w))
        });
        out
    }
}

/// Run `verb` on every target sequentially and summarize.
fn bulk_action(verb: &str, targets: &[(Machine, Job)], arg: Option<&str>) -> String {
    let mut ok = 0;
    let mut errors = Vec::new();
    for (m, j) in targets {
        match job_action_remote(m, verb, &j.id, arg) {
            Ok(_) => ok += 1,
            Err(e) => errors.push(format!("{}: {e}", j.id)),
        }
    }
    if errors.is_empty() {
        format!("{verb}: {ok} job(s) ok")
    } else {
        format!("{verb}: {ok} ok, {} failed — {}", errors.len(), errors.join("; "))
    }
}

/// Resolve the job's log path and start `tail -F` on the cluster. The
/// remote wrapper stops `tail` as soon as ssh's stdin closes, so killing
/// the local ssh never leaves an orphaned `tail` on the login node. Log
/// lines are streamed to the UI as `Msg::Log`.
fn start_follow(
    m: &Machine,
    job: &Job,
    finished: bool,
    gen: u64,
    tx: &Sender<Msg>,
) -> Result<(Child, String), String> {
    let info = job_info_raw(m, &job.id, finished)?;
    let path = detail_path(&info, true)
        .or_else(|| detail_path(&info, false))
        .ok_or("job record has no StdOut/StdErr path")?;
    if path.chars().any(char::is_control) {
        return Err("refusing log path with control characters".into());
    }
    if path.contains('%') {
        return Err(format!(
            "log path has an unexpanded pattern ({path}); select an array element"
        ));
    }
    let script = format!(
        "tail -n 100 -F -- {} & t=$!; cat >/dev/null; kill $t 2>/dev/null",
        shell_quote(&path)
    );
    let remote = format!("sh -c {}", shell_quote(&script));
    let args = ssh_argv(&m.ssh_target(), &remote);
    let mut child = new_command(&args[0])
        .args(&args[1..])
        .stdin(Stdio::piped()) // held open until the child is killed
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to start log follow: {e}"))?;
    let out = child.stdout.take().ok_or("no stdout pipe")?;
    let err = child.stderr.take().ok_or("no stderr pipe")?;
    spawn_line_reader(out, false, gen, tx.clone());
    spawn_line_reader(err, true, gen, tx.clone());
    Ok((child, path))
}

fn spawn_line_reader(pipe: impl std::io::Read + Send + 'static, err: bool, gen: u64, tx: Sender<Msg>) {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = clean_line(&String::from_utf8_lossy(&buf));
                    if tx.send(Msg::Log { gen, err, line }).is_err() {
                        return;
                    }
                }
            }
        }
        if !err {
            let _ = tx.send(Msg::Log {
                gen,
                err: true,
                line: "── stream ended ──".into(),
            });
        }
    });
}

fn spawn_refresher(tx: Sender<Msg>, scope: Arc<AtomicU8>, requests: Receiver<()>) {
    std::thread::spawn(move || loop {
        let current = Scope::from_u8(scope.load(Ordering::SeqCst));
        let data = load_machines().map(|machines| {
            let results = query_all(&machines, current);
            (machines, results)
        });
        if tx.send(Msg::Snapshot { scope: current, data }).is_err() {
            return; // pane closed
        }
        match requests.recv_timeout(Duration::from_secs(REFRESH_SECS)) {
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
            _ => while requests.try_recv().is_ok() {},
        }
    });
}

/// Raw mode + alternate screen, restored on every exit path.
struct TermGuard;

impl TermGuard {
    fn enter() -> TermGuard {
        let _ = crossterm::terminal::enable_raw_mode();
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::terminal::EnterAlternateScreen,
            crossterm::cursor::Hide
        );
        TermGuard
    }
}

impl Drop for TermGuard {
    fn drop(&mut self) {
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::cursor::Show,
            crossterm::terminal::LeaveAlternateScreen
        );
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

pub fn cmd_pane() {
    // The watcher must run for bindings to ever fire, pane open or not.
    watcher::cmd_start();
    let _guard = TermGuard::enter();
    let (tx, rx) = mpsc::channel::<Msg>();
    let (refresh_tx, refresh_rx) = mpsc::channel::<()>();
    let scope_flag = Arc::new(AtomicU8::new(Scope::Mine.to_u8()));
    spawn_refresher(tx.clone(), scope_flag.clone(), refresh_rx);
    let mut app = App {
        machines: Vec::new(),
        results: HashMap::new(),
        load_error: None,
        scope: Scope::Mine,
        scope_flag,
        refresh_tx,
        updated: None,
        loading: true,
        sel: None,
        cursor_hint: 0,
        marked: HashSet::new(),
        filter: String::new(),
        sort: Sort::Queue,
        status: String::new(),
        mode: Mode::List,
        armed: None,
        bindings: load_bindings(),
        tx,
        next_gen: 0,
        quit: false,
        list_top: std::cell::Cell::new(0),
    };
    let mut dirty = true;
    while !app.quit {
        while let Ok(msg) = rx.try_recv() {
            app.on_msg(msg);
            dirty = true;
        }
        if dirty {
            let mut stdout = std::io::stdout();
            let _ = stdout.write_all(app.draw().as_bytes());
            let _ = stdout.flush();
            dirty = false;
        }
        match event::poll(Duration::from_millis(100)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(k)) if k.kind != KeyEventKind::Release => {
                    app.on_key(k);
                    dirty = true;
                }
                Ok(Event::Resize(..)) => dirty = true,
                _ => {}
            },
            Ok(false) => {}
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: &str, name: &str, state: &str, elapsed: &str) -> Job {
        Job {
            id: id.into(),
            name: name.into(),
            state: state.into(),
            elapsed: elapsed.into(),
            nodes: "1".into(),
            account: "acc".into(),
            reason: String::new(),
            where_: "n1".into(),
        }
    }

    #[test]
    fn clean_line_strips_escapes_and_keeps_last_progress_frame() {
        assert_eq!(clean_line("\x1b[31mred\x1b[0m text\r\n"), "red text");
        assert_eq!(clean_line("10%\r50%\r100%\n"), "100%");
        assert_eq!(clean_line("a\tb\x07c"), "a    bc");
        assert_eq!(clean_line("\x1b]0;title\x07body"), "body");
    }

    #[test]
    fn wrap_splits_by_width_and_keeps_empty_lines() {
        assert_eq!(wrap("abcde", 2), vec!["ab", "cd", "e"]);
        assert_eq!(wrap("", 4), vec![String::new()]);
    }

    #[test]
    fn elapsed_parses_slurm_formats() {
        assert_eq!(elapsed_secs("1-02:03:04"), 86_400 + 7_384);
        assert_eq!(elapsed_secs("02:03:04"), 7_384);
        assert_eq!(elapsed_secs("03:04"), 184);
        assert_eq!(elapsed_secs("garbage"), 0);
    }

    #[test]
    fn filter_and_sort_order_jobs() {
        let a = job("1", "train", "PENDING", "00:00:00");
        let b = job("2", "eval", "RUNNING", "01:00:00");
        let c = job("3", "train2", "RUNNING", "02:00:00");
        assert!(matches_filter(&a, "TRAIN"));
        assert!(matches_filter(&a, "pend"));
        assert!(!matches_filter(&a, "eval"));
        let mut v = vec![&a, &b, &c];
        sort_jobs(&mut v, Sort::State);
        assert_eq!(v.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), ["2", "3", "1"]);
        sort_jobs(&mut v, Sort::Elapsed);
        assert_eq!(v.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), ["3", "2", "1"]);
        sort_jobs(&mut v, Sort::Name);
        assert_eq!(v.iter().map(|j| j.id.as_str()).collect::<Vec<_>>(), ["2", "1", "3"]);
    }
}
