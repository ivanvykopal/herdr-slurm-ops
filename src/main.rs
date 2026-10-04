use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime};

const REFRESH_SECS: u64 = 20;
const TARGET_COLS: usize = 50; // sidebar width used by the `open` subcommand
const SQUEUE_FMT: &str = "%.18i|%j|%T|%M|%D|%a|%R|%N";
const MAX_PBS_JSON_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCHEDULER_STDOUT_BYTES: usize = MAX_PBS_JSON_BYTES;
const MAX_SCHEDULER_STDERR_BYTES: usize = 1024 * 1024;

mod bindings;
mod pane;
mod watcher;

/// Which jobs the table shows. The watcher daemon always uses `Mine`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Mine,
    All,
    History,
}

impl Scope {
    fn to_u8(self) -> u8 {
        match self {
            Scope::Mine => 0,
            Scope::All => 1,
            Scope::History => 2,
        }
    }
    fn from_u8(v: u8) -> Scope {
        match v {
            1 => Scope::All,
            2 => Scope::History,
            _ => Scope::Mine,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Scope::Mine => "my jobs",
            Scope::All => "all users",
            Scope::History => "history 24h",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scheduler {
    Slurm,
    Pbs,
}

#[derive(Clone, Debug)]
struct Machine {
    name: String,
    host: String,
    scheduler: Scheduler,
    scheduler_user: String,
    scheduler_command: String,
    ssh_user: Option<String>,
    #[allow(dead_code)] // parsed for config parity with the monitor plugin
    session: String, // herdr session on the cluster; "" = default
}

impl Machine {
    fn ssh_target(&self) -> String {
        match &self.ssh_user {
            Some(user) if !user.is_empty() => format!("{user}@{}", self.host),
            _ => self.host.clone(),
        }
    }
}

#[derive(Clone)]
struct Job {
    id: String,
    name: String,
    state: String,
    elapsed: String,
    #[allow(dead_code)] // parsed from the queue, not shown in the ops table
    nodes: String,
    account: String,
    reason: String,
    where_: String,
}

type QueryResult = Result<Vec<Job>, String>;

// ------------------------------------------------------------------ config --

// This plugin does NOT own cluster configuration: it reads the monitor
// plugin's machines.toml so there is exactly one place to edit clusters.
fn slurm_config_dir() -> PathBuf {
    if let Ok(d) = env::var("HERDR_SLURM_MACHINES_DIR") {
        return PathBuf::from(d);
    }
    let args = vec![
        herdr_bin(),
        "plugin".into(),
        "config-dir".into(),
        "ivan.herdr-slurm".into(),
    ];
    if let Some(out) = run_capture(&args) {
        let t = out.trim();
        if !t.is_empty() {
            return PathBuf::from(t);
        }
    }
    let base = if cfg!(windows) {
        env::var("APPDATA").map(PathBuf::from).unwrap_or_default()
    } else if cfg!(target_os = "macos") {
        env::var("HOME")
            .map(|h| PathBuf::from(h).join("Library/Application Support"))
            .unwrap_or_default()
    } else {
        env::var("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|_| env::var("HOME").map(|h| PathBuf::from(h).join(".config")))
            .unwrap_or_default()
    };
    base.join("herdr")
        .join("plugins")
        .join("config")
        .join("ivan.herdr-slurm")
}

fn ops_config_dir() -> PathBuf {
    if let Ok(d) = env::var("HERDR_PLUGIN_CONFIG_DIR") {
        return PathBuf::from(d);
    }
    let base = if cfg!(windows) {
        env::var("APPDATA").map(PathBuf::from).unwrap_or_default()
    } else if cfg!(target_os = "macos") {
        env::var("HOME")
            .map(|h| PathBuf::from(h).join("Library/Application Support"))
            .unwrap_or_default()
    } else {
        env::var("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|_| env::var("HOME").map(|h| PathBuf::from(h).join(".config")))
            .unwrap_or_default()
    };
    base.join("herdr")
        .join("plugins")
        .join("config")
        .join("ivan.herdr-slurm-ops")
}

fn state_dir() -> PathBuf {
    if let Ok(d) = env::var("HERDR_PLUGIN_STATE_DIR") {
        return PathBuf::from(d);
    }
    ops_config_dir()
}

fn load_machines() -> Result<Vec<Machine>, String> {
    let path = slurm_config_dir().join("machines.toml");
    let raw = std::fs::read_to_string(&path).map_err(|e| {
        format!(
            "cannot read {} ({e}). Configure clusters in the herdr-slurm monitor plugin; this plugin reads its machines.toml.",
            path.display()
        )
    })?;
    let parsed = parse_machines(&raw).map_err(|e| format!("invalid machines config: {e}"))?;
    if parsed.machines.is_empty() {
        return Err(if parsed.warnings.is_empty() {
            format!("{} defines no machines", path.display())
        } else {
            parsed.warnings.join("; ")
        });
    }
    Ok(parsed.machines)
}

#[derive(Debug)]
enum ParseMachinesError {
    InvalidToml(String),
    NoValidMachines(String),
}

impl std::fmt::Display for ParseMachinesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidToml(error) | Self::NoValidMachines(error) => f.write_str(error),
        }
    }
}

#[derive(Debug)]
struct ParsedMachines {
    machines: Vec<Machine>,
    warnings: Vec<String>,
}

fn parse_machines(raw: &str) -> Result<ParsedMachines, ParseMachinesError> {
    let value: toml::Value =
        toml::from_str(raw).map_err(|e| ParseMachinesError::InvalidToml(e.to_string()))?;
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    if let Some(list) = value.get("machine").and_then(|v| v.as_array()) {
        for m in list {
            match parse_machine_entry(m) {
                Ok(machine) => out.push(machine),
                Err(error) => warnings.push(format!(
                    "skipping machine '{}': {error}",
                    machine_label(m)
                )),
            }
        }
    }
    if out.is_empty() {
        let details = if warnings.is_empty() {
            "no [[machine]] entries".to_string()
        } else {
            format!("no valid machines; {}", warnings.join("; "))
        };
        return Err(ParseMachinesError::NoValidMachines(details));
    }
    Ok(ParsedMachines {
        machines: out,
        warnings,
    })
}

fn machine_label(entry: &toml::Value) -> String {
    for key in ["name", "host"] {
        if let Some(value) = entry.get(key).and_then(|v| v.as_str()) {
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    "<unnamed>".to_string()
}

fn parse_machine_entry(entry: &toml::Value) -> Result<Machine, String> {
    let g = |k: &str| {
        entry
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let legacy_user = g("user");
    let scheduler = match g("scheduler").to_ascii_lowercase().as_str() {
        "" | "slurm" => Scheduler::Slurm,
        "pbs" | "openpbs" => Scheduler::Pbs,
        other => return Err(format!("unsupported scheduler '{other}'")),
    };
    let scheduler_command = match g("scheduler_command") {
        value if value.is_empty() => match scheduler {
            Scheduler::Slurm => "squeue".into(),
            Scheduler::Pbs => "qstat".into(),
        },
        value => value,
    };
    if scheduler_command.starts_with('-')
        || !scheduler_command
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '\\' | '_' | '-' | '.'))
    {
        return Err(format!("invalid scheduler_command '{scheduler_command}'"));
    }
    let scheduler_user = match g("scheduler_user") {
        value if value.is_empty() => legacy_user,
        value => value,
    };
    if scheduler_user.is_empty()
        || !scheduler_user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(format!("invalid scheduler_user '{scheduler_user}'"));
    }
    let ssh_user = match g("ssh_user") {
        value if value.is_empty() => None,
        value => Some(value),
    };
    let host = g("host");
    if host.is_empty()
        || host.starts_with('-')
        || !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':' | '[' | ']'))
    {
        return Err(format!("invalid host '{host}'"));
    }
    if let Some(user) = &ssh_user {
        if user.starts_with('-')
            || !user
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            return Err(format!("invalid ssh_user '{user}'"));
        }
    }
    let session = g("session");
    if !session.is_empty()
        && !session
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(format!("invalid session '{session}'"));
    }
    Ok(Machine {
        name: g("name"),
        host,
        scheduler,
        scheduler_user,
        scheduler_command,
        ssh_user,
        session,
    })
}

/// A job id that is safe to interpolate into a remote scheduler command.
/// squeue ids are numeric; Slurm array rows look like `123_4` (element) or
/// `123_[4-8%2]` (pending range). The charset below covers those and cannot
/// form shell metacharacters; ids are additionally single-quoted in every
/// remote command so the cluster shell cannot glob-expand `[...]`.
fn valid_job_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 32
        && id.chars().all(|c| {
            c.is_ascii_digit() || matches!(c, '_' | '-' | '.' | ',' | '%' | '[' | ']')
        })
        && id.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false)
        && id.chars().any(|c| c.is_ascii_digit())
        && !id.contains('\'')
}

/// The id to hand to `scontrol show job`: an array row like `94415_[8-12%1]`
/// is not a valid scontrol argument, but its master (`94415`) is and shows
/// the whole array. Element ids (`94415_7`) pass through unchanged.
fn detail_job_id(id: &str) -> String {
    if let Some((base, _)) = id.split_once('[') {
        return base.trim_end_matches('_').to_string();
    }
    id.to_string()
}

// -------------------------------------------------------------------- data --

const SACCT_FMT: &str = "JobID,JobName,State,Elapsed,NNodes,Account,ExitCode,NodeList";

fn remote_command(machine: &Machine, scope: Scope) -> String {
    let cmd = &machine.scheduler_command;
    let user = &machine.scheduler_user;
    match (machine.scheduler, scope) {
        (Scheduler::Slurm, Scope::Mine) => format!("{cmd} -u {user} -h -o '{SQUEUE_FMT}'"),
        (Scheduler::Slurm, Scope::All) => format!("{cmd} -h -o '{SQUEUE_FMT}'"),
        (Scheduler::Slurm, Scope::History) => {
            format!("sacct -u {user} -X -n -P -S now-1days -o {SACCT_FMT}")
        }
        (Scheduler::Pbs, Scope::History) => format!("{cmd} -x -f -F json"),
        (Scheduler::Pbs, _) => format!("{cmd} -f -F json"),
    }
}

/// ssh executable; overridable so wrappers (or tests) can substitute it.
fn ssh_bin() -> String {
    env::var("HERDR_SLURM_OPS_SSH").unwrap_or_else(|_| "ssh".into())
}

// argv for running a command on the cluster over ssh. The local CLI's
// remote path (`herdr --machine <label> ...`) is NOT used on purpose: on
// Windows it allocates a visible console window whenever the caller has no
// console (the detached daemon), flashing a window every poll cycle.
fn ssh_argv(target: &str, remote: &str) -> Vec<String> {
    let mut v: Vec<String> = [
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=8",
        "-o",
        "LogLevel=ERROR",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    v.insert(0, ssh_bin());
    v.push(target.to_string());
    v.push(remote.to_string());
    v
}

/// Run one remote scheduler command, bounded like the monitor's queries.
fn ssh_run(machine: &Machine, remote: &str) -> Result<String, String> {
    let args = ssh_argv(&machine.ssh_target(), remote);
    let mut command = new_command(&args[0]);
    command.args(&args[1..]).stderr(Stdio::piped());
    let out = run_command_with_timeout(&mut command, Duration::from_secs(15))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let last = err.lines().last().unwrap_or("remote command failed");
        return Err(truncate(last, 100));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn query_machine(machine: &Machine, scope: Scope) -> QueryResult {
    let text = ssh_run(machine, &remote_command(machine, scope))?;
    match (machine.scheduler, scope) {
        (Scheduler::Pbs, Scope::All) => parse_pbs_jobs(&text, None),
        (Scheduler::Pbs, _) => parse_pbs_jobs(&text, Some(&machine.scheduler_user)),
        (Scheduler::Slurm, Scope::History) => Ok(parse_sacct_jobs(&text)),
        (Scheduler::Slurm, _) => Ok(parse_slurm_jobs(&text)),
    }
}

/// Query every cluster concurrently: a slow login node must not delay the
/// other clusters' results.
fn query_all(machines: &[Machine], scope: Scope) -> HashMap<String, QueryResult> {
    std::thread::scope(|s| {
        let handles: Vec<_> = machines
            .iter()
            .map(|m| (m.name.clone(), s.spawn(move || query_machine(m, scope))))
            .collect();
        handles
            .into_iter()
            .map(|(name, h)| {
                let res = h
                    .join()
                    .unwrap_or_else(|_| Err("query thread panicked".into()));
                (name, res)
            })
            .collect()
    })
}

fn run_command_with_timeout(command: &mut Command, timeout: Duration) -> Result<Output, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command
        .spawn()
        .map_err(|_| "ssh not found in PATH".to_string())?;
    let mut stdout = child.stdout.take().ok_or("failed to capture stdout")?;
    let mut stderr = child.stderr.take().ok_or("failed to capture stderr")?;
    let stdout_reader =
        std::thread::spawn(move || read_bounded(&mut stdout, MAX_SCHEDULER_STDOUT_BYTES));
    let stderr_reader =
        std::thread::spawn(move || read_bounded(&mut stderr, MAX_SCHEDULER_STDERR_BYTES));
    let started = Instant::now();
    let mut status = None;
    loop {
        match child.try_wait() {
            Ok(Some(exit_status)) => status = Some(exit_status),
            Ok(None) => {}
            Err(error) => {
                terminate_command(&mut child);
                let _ = child.wait();
                return Err(error.to_string());
            }
        }
        if status.is_some() && stdout_reader.is_finished() && stderr_reader.is_finished() {
            break;
        }
        if started.elapsed() >= timeout {
            terminate_command(&mut child);
            if status.is_none() {
                let _ = child.wait();
            }
            return Err(format!("scheduler query timed out after {}s", timeout.as_secs()));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let (stdout, stdout_exceeded) = stdout_reader
        .join()
        .map_err(|_| "stdout reader panicked".to_string())?
        .map_err(|error| error.to_string())?;
    let (stderr, stderr_exceeded) = stderr_reader
        .join()
        .map_err(|_| "stderr reader panicked".to_string())?
        .map_err(|error| error.to_string())?;

    let status = status.ok_or("scheduler command exited without a status")?;
    if stdout_exceeded {
        return Err(format!(
            "scheduler stdout exceeded {MAX_SCHEDULER_STDOUT_BYTES} bytes"
        ));
    }
    if stderr_exceeded {
        return Err(format!(
            "scheduler stderr exceeded {MAX_SCHEDULER_STDERR_BYTES} bytes"
        ));
    }

    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn read_bounded(reader: &mut impl Read, limit: usize) -> std::io::Result<(Vec<u8>, bool)> {
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0_u8; 64 * 1024];
    let mut exceeded = false;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        bytes.extend_from_slice(&buffer[..count.min(remaining)]);
        exceeded |= count > remaining;
    }
    Ok((bytes, exceeded))
}

fn terminate_command(child: &mut std::process::Child) {
    #[cfg(unix)]
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
}

fn parse_slurm_jobs(text: &str) -> Vec<Job> {
    let mut jobs = Vec::new();
    for line in text.lines() {
        let parts: Vec<&str> = line.split('|').map(str::trim).collect();
        if parts.len() != 8 {
            continue;
        }
        jobs.push(Job {
            id: parts[0].into(),
            name: parts[1].into(),
            state: parts[2].into(),
            elapsed: parts[3].into(),
            nodes: parts[4].into(),
            account: parts[5].into(),
            reason: parts[6].into(),
            where_: parts[7].into(),
        });
    }
    jobs
}

/// `sacct -P -n` rows (SACCT_FMT) as table rows for the history view.
fn parse_sacct_jobs(text: &str) -> Vec<Job> {
    let mut jobs = Vec::new();
    for line in text.lines() {
        let parts: Vec<&str> = line.split('|').map(str::trim).collect();
        if parts.len() != 8 || parts[0].is_empty() {
            continue;
        }
        jobs.push(Job {
            id: parts[0].into(),
            name: parts[1].into(),
            // "CANCELLED by 1234" -> CANCELLED
            state: parts[2].split_whitespace().next().unwrap_or("").into(),
            elapsed: parts[3].into(),
            nodes: parts[4].into(),
            account: parts[5].into(),
            reason: format!("exit {}", parts[6]),
            where_: parts[7].into(),
        });
    }
    jobs
}

fn pbs_exit_status(job: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    job.get("Exit_status")
        .and_then(json_scalar)
        .map(|s| s.trim().to_string())
}

/// Normalized PBS state; a finished job with a non-zero exit status is FAILED.
fn pbs_job_state(job: &serde_json::Map<String, serde_json::Value>) -> String {
    let raw = job.get("job_state").and_then(|v| v.as_str()).unwrap_or("");
    if matches!(raw, "F" | "X") && pbs_exit_status(job).is_some_and(|c| c != "0") {
        return "FAILED".into();
    }
    normalize_pbs_state(raw)
}

fn parse_pbs_jobs(text: &str, owner_filter: Option<&str>) -> QueryResult {
    let repaired = repair_pbs_json(text)?;
    let root: serde_json::Value = serde_json::from_str(&repaired).map_err(|e| e.to_string())?;
    let jobs = root
        .get("Jobs")
        .and_then(|value| value.as_object())
        .ok_or("qstat JSON has no Jobs object")?;
    let mut parsed = Vec::new();
    for (id, value) in jobs {
        let Some(job) = value.as_object() else {
            continue;
        };
        let owner = job
            .get("Job_Owner")
            .and_then(|value| value.as_str())
            .or_else(|| job.get("euser").and_then(|value| value.as_str()))
            .or_else(|| {
                job.get("Variable_List")
                    .and_then(|value| value.as_object())
                    .and_then(|variables| variables.get("PBS_O_LOGNAME"))
                    .and_then(|value| value.as_str())
            });
        if let Some(user) = owner_filter {
            if owner.and_then(|owner| owner.split('@').next()) != Some(user) {
                continue;
            }
        }
        let string = |key: &str| {
            job.get(key)
                .and_then(|value| value.as_str())
                .map(sanitize_display_text)
                .unwrap_or_default()
        };
        let resources = job.get("Resource_List").and_then(|value| value.as_object());
        let nodes = resources
            .and_then(|resources| resources.get("nodect"))
            .and_then(json_scalar)
            .or_else(|| {
                resources
                    .and_then(|resources| resources.get("select"))
                    .and_then(|value| value.as_str())
                    .map(nodes_from_select)
            })
            .unwrap_or_default();
        let account = ["Account_Name", "project"]
            .iter()
            .find_map(|key| job.get(*key).and_then(json_scalar))
            .unwrap_or_default();
        let elapsed = job
            .get("resources_used")
            .and_then(|value| value.as_object())
            .and_then(|resources| resources.get("walltime"))
            .and_then(json_scalar)
            .unwrap_or_default();
        let state = pbs_job_state(job);
        parsed.push(Job {
            id: sanitize_display_text(id),
            name: string("Job_Name"),
            state,
            elapsed,
            nodes,
            account,
            reason: string("comment"),
            where_: string("exec_host"),
        });
    }
    Ok(parsed)
}

fn sanitize_display_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

fn repair_pbs_json(text: &str) -> Result<String, String> {
    if text.len() > MAX_PBS_JSON_BYTES {
        return Err(format!(
            "qstat JSON is too large ({} bytes; limit is {MAX_PBS_JSON_BYTES})",
            text.len()
        ));
    }

    let chars: Vec<char> = text.chars().collect();
    let mut repaired = String::with_capacity(text.len());
    let mut in_string = false;
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if in_string {
            match ch {
                '"' => {
                    in_string = false;
                    repaired.push(ch);
                }
                '\\' if index + 1 < chars.len() => {
                    let escaped = chars[index + 1];
                    let valid_unicode = escaped == 'u'
                        && index + 5 < chars.len()
                        && chars[index + 2..=index + 5]
                            .iter()
                            .all(|digit| digit.is_ascii_hexdigit());
                    if matches!(escaped, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't')
                        || valid_unicode
                    {
                        repaired.push(ch);
                        repaired.push(escaped);
                        index += 2;
                        continue;
                    }
                    repaired.push_str("\\\\");
                }
                c if c.is_control() => repaired.push(' '),
                _ => repaired.push(ch),
            }
            index += 1;
            continue;
        }

        if ch == '"' {
            in_string = true;
            repaired.push(ch);
            index += 1;
            continue;
        }
        if ch.is_control() {
            repaired.push(' ');
            index += 1;
            continue;
        }

        let token_len = [
            "-infinity", "+infinity", "infinity", "-inf", "+inf", "nan", "inf",
        ]
        .iter()
        .find(|token| {
            let token_len = token.len();
            index + token_len <= chars.len()
                && chars[index..index + token_len]
                    .iter()
                    .zip(token.chars())
                    .all(|(actual, expected)| actual.eq_ignore_ascii_case(&expected))
        })
        .map(|token| token.len());
        if let Some(len) = token_len {
            let before_ok = index == 0 || !chars[index - 1].is_ascii_alphanumeric();
            let after = index + len;
            let after_ok = after >= chars.len() || !chars[after].is_ascii_alphanumeric();
            if before_ok && after_ok {
                repaired.push_str("null");
                index += len;
                continue;
            }
        }
        repaired.push(ch);
        index += 1;
    }
    Ok(repaired)
}

fn json_scalar(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn nodes_from_select(select: &str) -> String {
    select
        .split('+')
        .map(|chunk| {
            chunk
                .split(':')
                .next()
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(1)
        })
        .sum::<u64>()
        .to_string()
}

fn normalize_pbs_state(state: &str) -> String {
    match state {
        "R" | "B" => "RUNNING".into(),
        "Q" | "W" => "PENDING".into(),
        "H" | "S" | "U" => "SUSPENDED".into(),
        "E" => "COMPLETING".into(),
        "C" | "F" | "X" => "COMPLETED".into(),
        "T" => "TRANSIT".into(),
        "M" => "MOVED".into(),
        other => format!("UNKNOWN({other})"),
    }
}

fn truncate(s: &str, n: usize) -> String {
    match s.char_indices().nth(n) {
        Some((i, _)) => s[..i].to_string(),
        None => s.to_string(),
    }
}

fn state_col(s: &str) -> &'static str {
    match s {
        "RUNNING" => "\x1b[32m",
        "PENDING" => "\x1b[33m",
        "COMPLETING" => "\x1b[36m",
        "SUSPENDED" => "\x1b[35m",
        "FAILED" | "CANCELLED" | "TIMEOUT" | "OUT_OF_MEMORY" => "\x1b[31m",
        _ => "\x1b[90m",
    }
}

fn pad(s: &str, width: usize) -> String {
    let mut out = s.to_string();
    while out.chars().count() < width {
        out.push(' ');
    }
    out
}

fn pad_left(s: &str, width: usize) -> String {
    let mut out = String::new();
    while out.chars().count() + s.chars().count() < width {
        out.push(' ');
    }
    out.push_str(s);
    out
}

// ------------------------------------------------------------------- herdr --

fn herdr_bin() -> String {
    env::var("HERDR_BIN_PATH").unwrap_or_else(|_| "herdr".into())
}

// Every child this program spawns is capture-only (stdout/stderr piped or
// null). On Windows, console-subsystem children (ssh, herdr) launched from
// the console-less daemon would each allocate a fresh, visible console
// window; CREATE_NO_WINDOW prevents that. It is harmless when a console
// already exists (pane, action), so apply it unconditionally.
fn new_command<S: AsRef<std::ffi::OsStr>>(prog: S) -> Command {
    let mut cmd = Command::new(prog);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd
}

fn run_capture(args: &[String]) -> Option<String> {
    let out = new_command(&args[0]).args(&args[1..]).output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        None
    }
}

// ------------------------------------------------------------- job control --

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Slurm/PBS time limits: `30`, `2:00:00`, `1-12:00:00`, `+30`, `UNLIMITED`.
fn valid_time_limit(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 24
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '-' | '+'))
}

/// `verb`: cancel | hold | release | requeue | timelimit (needs `arg`).
fn job_action_remote(
    machine: &Machine,
    verb: &str,
    job_id: &str,
    arg: Option<&str>,
) -> Result<String, String> {
    if !valid_job_id(job_id) {
        return Err(format!("invalid job id '{job_id}'"));
    }
    let qid = shell_quote(job_id); // single-quoted: the cluster shell cannot glob `[...]`
    let limit = || {
        arg.filter(|v| valid_time_limit(v))
            .map(shell_quote)
            .ok_or_else(|| "invalid time limit".to_string())
    };
    let remote = match (machine.scheduler, verb) {
        (Scheduler::Slurm, "cancel") => format!("scancel {qid}"),
        (Scheduler::Slurm, "hold") => format!("scontrol hold {qid}"),
        (Scheduler::Slurm, "release") => format!("scontrol release {qid}"),
        (Scheduler::Slurm, "requeue") => format!("scontrol requeue {qid}"),
        (Scheduler::Slurm, "timelimit") => {
            format!("scontrol update JobId={qid} TimeLimit={}", limit()?)
        }
        (Scheduler::Pbs, "cancel") => format!("qdel {qid}"),
        (Scheduler::Pbs, "hold") => format!("qhold {qid}"),
        (Scheduler::Pbs, "release") => format!("qrls {qid}"),
        (Scheduler::Pbs, "requeue") => format!("qrerun {qid}"),
        (Scheduler::Pbs, "timelimit") => format!("qalter -l walltime={} {qid}", limit()?),
        _ => return Err(format!("unsupported action '{verb}'")),
    };
    ssh_run(machine, &remote)
}

/// Raw scheduler description of one job: `scontrol show job` (Slurm) or
/// `qstat -f` (PBS; `-x` for finished jobs).
fn job_info_raw(machine: &Machine, job_id: &str, finished: bool) -> Result<String, String> {
    if !valid_job_id(job_id) {
        return Err(format!("invalid job id '{job_id}'"));
    }
    let qid = shell_quote(&detail_job_id(job_id));
    let remote = match machine.scheduler {
        Scheduler::Slurm => format!("scontrol show job {qid}"),
        Scheduler::Pbs if finished => format!("{} -f -x {qid}", machine.scheduler_command),
        Scheduler::Pbs => format!("{} -f {qid}", machine.scheduler_command),
    };
    ssh_run(machine, &remote)
}

/// Detail view text. Finished Slurm jobs come from accounting (`sacct`),
/// everything else from the live scheduler record.
fn job_detail_remote(machine: &Machine, job_id: &str, finished: bool) -> Result<String, String> {
    if finished && machine.scheduler == Scheduler::Slurm {
        if !valid_job_id(job_id) {
            return Err(format!("invalid job id '{job_id}'"));
        }
        let qid = shell_quote(&detail_job_id(job_id));
        let text = ssh_run(
            machine,
            &format!(
                "sacct -j {qid} -P -o JobID,JobName,Partition,Account,AllocCPUS,ReqMem,State,ExitCode,Elapsed,Timelimit,Start,End,NodeList,MaxRSS"
            ),
        )?;
        let out = format_sacct_detail(&text);
        return Ok(if out.is_empty() {
            "no accounting record".into()
        } else {
            out
        });
    }
    job_info_raw(machine, job_id, finished)
}

/// `sacct -P` (header + rows) as `Column: value` blocks.
fn format_sacct_detail(text: &str) -> String {
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return String::new();
    };
    let cols: Vec<&str> = header.split('|').collect();
    let mut out = String::new();
    for row in lines.filter(|l| !l.trim().is_empty()) {
        for (col, value) in cols.iter().zip(row.split('|')) {
            if !value.is_empty() {
                out.push_str(&format!("{col:>10}: {value}\n"));
            }
        }
        out.push('\n');
    }
    out
}

/// Partition summary appended to the detail view (Slurm only).
fn sinfo_summary_remote(machine: &Machine) -> Result<String, String> {
    if machine.scheduler != Scheduler::Slurm {
        return Ok(String::new());
    }
    ssh_run(machine, "sinfo -h -o '%P %a %D %t'")
}

/// Extract a file path from a job detail blob. Slurm: `StdOut=...` /
/// `StdErr=...` (the whole remainder of the line: paths may hold spaces
/// and `:`/`=`). PBS: `Output_Path = [host:]/path` / `Error_Path = ...`,
/// where qstat wraps long values onto tab-indented continuation lines.
fn detail_path(detail: &str, stdout_not_stderr: bool) -> Option<String> {
    let (slurm_key, pbs_key) = if stdout_not_stderr {
        ("StdOut=", "Output_Path")
    } else {
        ("StdErr=", "Error_Path")
    };
    let mut logical: Vec<String> = Vec::new();
    for raw in detail.lines() {
        match logical.last_mut() {
            Some(prev) if raw.starts_with('\t') => prev.push_str(raw.trim()),
            _ => logical.push(raw.trim().to_string()),
        }
    }
    for line in &logical {
        if let Some(rest) = line.strip_prefix(slurm_key) {
            if !rest.trim().is_empty() {
                return Some(rest.trim().to_string());
            }
        }
        if let Some(rest) = line.strip_prefix(pbs_key) {
            let Some(value) = rest.trim_start().strip_prefix('=') else {
                continue;
            };
            let value = value.trim();
            // `host:/path` -> `/path`; a bare path (even with ':') is kept.
            let path = match value.split_once(':') {
                Some((host, p)) if !host.contains('/') && p.starts_with('/') => p,
                _ => value,
            };
            if !path.is_empty() {
                return Some(path.to_string());
            }
        }
    }
    None
}

/// Terminal states after which a job never changes again (Slurm names; PBS
/// normalizes to the same set).
fn is_terminal_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED"
            | "FAILED"
            | "CANCELLED"
            | "TIMEOUT"
            | "OUT_OF_MEMORY"
            | "NODE_FAIL"
            | "BOOT_FAIL"
            | "DEADLINE"
    )
}

/// The one-time terminal-state fact delivered to the bound agent.
fn terminal_message(machine: &str, job_id: &str, state: &str, exit: &str) -> String {
    if state == "UNKNOWN" {
        return format!(
            "[scheduler] job {job_id} on {machine} left the queue; final state unavailable."
        );
    }
    format!(
        "[scheduler] job {job_id} on {machine} finished: {state}{}.",
        if exit.is_empty() {
            String::new()
        } else {
            format!(" (exit {exit})")
        }
    )
}

fn notify_agent(pane_id: &str, text: &str) -> bool {
    let args = vec![
        herdr_bin(),
        "agent".into(),
        "prompt".into(),
        pane_id.into(),
        text.into(),
    ];
    run_capture(&args).is_some()
}

fn desktop_notify(title: &str, body: &str, sound_done: bool) {
    let mut args = vec![
        herdr_bin(),
        "notification".into(),
        "show".into(),
        title.into(),
        "--body".into(),
        body.into(),
    ];
    if sound_done {
        args.push("--sound".into());
        args.push("done".into());
    }
    let _ = run_capture(&args);
}

#[derive(Clone, Debug)]
struct AgentInfo {
    pane: String,
    agent: String,
    title: String,
}

/// `herdr agent list` -> the agents the `n` picker offers.
fn list_agents() -> Result<Vec<AgentInfo>, String> {
    let args = vec![herdr_bin(), "agent".into(), "list".into()];
    let out = run_capture(&args).ok_or("`herdr agent list` failed")?;
    let v: serde_json::Value =
        serde_json::from_str(&out).map_err(|_| "unparseable `herdr agent list` output")?;
    let mut agents = Vec::new();
    if let Some(list) = v.pointer("/result/agents").and_then(|a| a.as_array()) {
        for a in list {
            let Some(pane) = a.get("pane_id").and_then(|p| p.as_str()) else {
                continue;
            };
            let text = |k: &str, d: &str| {
                a.get(k)
                    .and_then(|x| x.as_str())
                    .unwrap_or(d)
                    .to_string()
            };
            agents.push(AgentInfo {
                pane: pane.to_string(),
                agent: text("agent", "?"),
                title: text("terminal_title_stripped", ""),
            });
        }
    }
    Ok(agents)
}

fn local_timestamp(t: &SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Local> = (*t).into();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

// ------------------------------------------------------------------ actions --

fn cmd_refresh() {
    let machines = match load_machines() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("herdr-slurm-ops: {e}");
            std::process::exit(1);
        }
    };
    let mut results = query_all(&machines, Scope::Mine);
    for m in &machines {
        println!("▸ {} ({} on {})", m.name, m.scheduler_user, m.ssh_target());
        match results.remove(&m.name).unwrap_or(Ok(Vec::new())) {
            Err(e) => println!("  ✗ {e}"),
            Ok(jobs) if jobs.is_empty() => println!("  (no jobs in queue)"),
            Ok(jobs) => {
                for j in &jobs {
                    println!(
                        "  {} {} {} {} {}  {}",
                        pad(&truncate(&j.id, 16), 16),
                        pad(&truncate(&j.name, 20), 20),
                        pad(&truncate(&j.account, 12), 12),
                        pad(&truncate(&j.state, 11), 11),
                        pad_left(&j.elapsed, 10),
                        j.where_
                    );
                }
            }
        }
    }
}

fn cmd_open() {
    let herdr = herdr_bin();
    let entrypoint = if cfg!(windows) {
        "ops-windows"
    } else {
        "ops"
    };
    let plugin = env::var("HERDR_PLUGIN_ID").unwrap_or_else(|_| "ivan.herdr-slurm-ops".into());
    let out = match new_command(&herdr)
        .args([
            "plugin",
            "pane",
            "open",
            "--plugin",
            &plugin,
            "--entrypoint",
            entrypoint,
        ])
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("herdr-slurm-ops: failed to run {herdr}: {e}");
            std::process::exit(1);
        }
    };
    if !out.status.success() {
        eprint!("{}", String::from_utf8_lossy(&out.stderr));
        std::process::exit(out.status.code().unwrap_or(1));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let pane_id = serde_json::from_str::<serde_json::Value>(&stdout)
        .ok()
        .and_then(|v| {
            v.pointer("/result/plugin_pane/pane/pane_id")
                .and_then(|p| p.as_str())
                .map(str::to_string)
        });
    let pane_id = match pane_id {
        Some(p) => p,
        None => return, // already open
    };
    // The split layout can take a moment to settle after `pane open`.
    let mut last_err = String::new();
    for _ in 0..5 {
        std::thread::sleep(Duration::from_millis(200));
        match resize_sidebar(&herdr, &pane_id) {
            Ok(()) => return,
            Err(e) => last_err = e,
        }
    }
    eprintln!("herdr-slurm-ops: could not resize pane to sidebar width: {last_err}");
}

fn resize_sidebar(herdr: &str, pane_id: &str) -> Result<(), String> {
    let stdout = run_capture(&[herdr.to_string(), "pane".into(), "edges".into()])
        .ok_or("pane edges failed")?;
    let v: serde_json::Value = serde_json::from_str(&stdout).map_err(|e| e.to_string())?;
    let layout = v.pointer("/result/edges/layout").ok_or("no layout")?;
    let area_w = layout
        .pointer("/area/width")
        .and_then(|w| w.as_u64())
        .ok_or("no area")? as i64;
    let (x, width) = layout
        .pointer("/panes")
        .and_then(|p| p.as_array())
        .and_then(|panes| {
            panes
                .iter()
                .find(|p| p.get("pane_id").and_then(|i| i.as_str()) == Some(pane_id))
        })
        .and_then(|p| {
            Some((
                p.pointer("/rect/x").and_then(|v| v.as_i64())?,
                p.pointer("/rect/width").and_then(|v| v.as_i64())?,
            ))
        })
        .ok_or("pane not found in layout")?;
    if (width - TARGET_COLS as i64).abs() < 3 {
        return Ok(());
    }
    let target_x = (area_w - TARGET_COLS as i64).max(0);
    let delta = target_x - x;
    if delta.abs() < 2 {
        return Ok(());
    }
    let direction = if delta > 0 { "right" } else { "left" };
    let amount = (delta.abs() as f64 / area_w as f64).min(0.45);
    let out = new_command(herdr)
        .args([
            "pane",
            "resize",
            "--pane",
            pane_id,
            "--direction",
            direction,
            "--amount",
        ])
        .arg(format!("{amount:.4}"))
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err("resize failed".into())
    }
}

// --------------------------------------------------------------------- main --

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("open") => cmd_open(),
        Some("refresh") => cmd_refresh(),
        Some("start") => watcher::cmd_start(),
        Some("stop") => watcher::cmd_stop(),
        Some("daemon") => watcher::cmd_daemon(),
        _ => pane::cmd_pane(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::*;

    #[test]
    fn job_ids_are_validated_before_reaching_ssh() {
        assert!(valid_job_id("12345"));
        assert!(valid_job_id("123_4"));
        assert!(valid_job_id("94415_[8-12%1]"));
        assert!(valid_job_id("94415_[2,5-7]"));
        assert!(!valid_job_id(""));
        assert!(!valid_job_id("123; reboot"));
        assert!(!valid_job_id("$(rm -rf /)"));
        assert!(!valid_job_id("-x"));
        assert!(!valid_job_id("abc"));
        assert!(!valid_job_id("12'34"));
    }

    #[test]
    fn array_rows_resolve_to_master_for_scontrol() {
        assert_eq!(detail_job_id("94415_[8-12%1]"), "94415");
        assert_eq!(detail_job_id("94415_[2,5-7]"), "94415");
        assert_eq!(detail_job_id("94415_7"), "94415_7");
        assert_eq!(detail_job_id("94415"), "94415");
    }

    #[test]
    fn slurm_detail_path_extraction() {
        let detail = "JobId=123 JobName=train\n\
                      StdOut=/home/user/slurm-123.out\n\
                      StdErr=/home/user/slurm-123.err\n";
        assert_eq!(
            detail_path(detail, true).as_deref(),
            Some("/home/user/slurm-123.out")
        );
        assert_eq!(
            detail_path(detail, false).as_deref(),
            Some("/home/user/slurm-123.err")
        );
    }

    #[test]
    fn detail_path_keeps_spaces_colons_and_equals() {
        let detail = "   StdOut=/scratch/my runs/a=b:c/slurm-1.out\n";
        assert_eq!(
            detail_path(detail, true).as_deref(),
            Some("/scratch/my runs/a=b:c/slurm-1.out")
        );
    }

    #[test]
    fn pbs_detail_path_extraction_strips_host_prefix() {
        let detail = "Output_Path = login01:/home/user/job.out\n\
                      Error_Path = login01:/home/user/job.err\n";
        assert_eq!(detail_path(detail, true).as_deref(), Some("/home/user/job.out"));
        assert_eq!(detail_path(detail, false).as_deref(), Some("/home/user/job.err"));
    }

    #[test]
    fn pbs_detail_path_joins_wrapped_values() {
        let detail = "    Output_Path = login01:/home/user/very/long/\n\tpath/job.out\n    Error_Path = /e\n";
        assert_eq!(
            detail_path(detail, true).as_deref(),
            Some("/home/user/very/long/path/job.out")
        );
    }

    #[test]
    fn shell_quote_survives_single_quotes() {
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn time_limits_are_restricted_to_scheduler_syntax() {
        assert!(valid_time_limit("2:00:00"));
        assert!(valid_time_limit("1-12:00:00"));
        assert!(valid_time_limit("+30"));
        assert!(!valid_time_limit(""));
        assert!(!valid_time_limit("1; reboot"));
        assert!(!valid_time_limit("$(x)"));
    }

    #[test]
    fn sacct_rows_become_history_jobs() {
        let text = "9|train|CANCELLED by 12|01:00:00|1|acc|0:15|n1\nshort|row\n";
        let jobs = parse_sacct_jobs(text);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, "CANCELLED");
        assert_eq!(jobs[0].reason, "exit 0:15");
    }

    #[test]
    fn bindings_round_trip() {
        let doc = serde_json::json!({
            "bindings": [
                {"machine": "Cluster", "job_id": "42", "pane_id": "w1A:p1", "agent": "omp"},
                {"machine": "", "job_id": "1", "pane_id": "x"},  // invalid: dropped
            ]
        })
        .to_string();
        let b = parse_bindings(&doc);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].job_id, "42");
        assert!(binding_matches(&b[0], "Cluster", "42"));
        assert!(!binding_matches(&b[0], "Cluster", "43"));
    }

    #[test]
    fn terminal_states_match_both_schedulers() {
        assert!(is_terminal_state("COMPLETED"));
        assert!(is_terminal_state("FAILED"));
        assert!(is_terminal_state("TIMEOUT"));
        assert!(is_terminal_state("CANCELLED"));
        assert!(!is_terminal_state("RUNNING"));
        assert!(!is_terminal_state("PENDING"));
        assert!(!is_terminal_state("REQUEUED"));
        // PBS normalization feeds the same set
        assert!(is_terminal_state(&normalize_pbs_state("F")));
        assert!(is_terminal_state(&normalize_pbs_state("C")));
    }

    #[test]
    fn finished_pbs_job_with_nonzero_exit_is_failed() {
        let doc: serde_json::Value = serde_json::json!({"job_state": "F", "Exit_status": 271});
        assert_eq!(pbs_job_state(doc.as_object().unwrap()), "FAILED");
        let ok: serde_json::Value = serde_json::json!({"job_state": "F", "Exit_status": 0});
        assert_eq!(pbs_job_state(ok.as_object().unwrap()), "COMPLETED");
        let running: serde_json::Value = serde_json::json!({"job_state": "R"});
        assert_eq!(pbs_job_state(running.as_object().unwrap()), "RUNNING");
    }

    #[test]
    fn terminal_message_shape() {
        assert_eq!(
            terminal_message("C", "9", "COMPLETED", ""),
            "[scheduler] job 9 on C finished: COMPLETED."
        );
        assert_eq!(
            terminal_message("C", "9", "FAILED", "2:0"),
            "[scheduler] job 9 on C finished: FAILED (exit 2:0)."
        );
        assert!(terminal_message("C", "9", "UNKNOWN", "").contains("final state unavailable"));
    }

    #[test]
    fn slurm_job_parsing_matches_monitor_format() {
        let text = "123|train|RUNNING|01:02:03|2|acc|None|node[1-2]\nbadline\n";
        let jobs = parse_slurm_jobs(text);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "123");
        assert_eq!(jobs[0].state, "RUNNING");
        assert_eq!(jobs[0].where_, "node[1-2]");
    }

    #[test]
    fn machines_config_is_shared_with_monitor_plugin() {
        // The ops parser accepts the monitor plugin's documented config
        // shape unchanged, including legacy `user`.
        let raw = r#"
[[machine]]
name = "C"
host = "login.example.org"
scheduler = "slurm"
user = "cluster-user"
"#;
        let parsed = parse_machines(raw).unwrap();
        assert_eq!(parsed.machines.len(), 1);
        assert_eq!(parsed.machines[0].scheduler_user, "cluster-user");
        assert_eq!(parsed.machines[0].scheduler, Scheduler::Slurm);
    }
}
