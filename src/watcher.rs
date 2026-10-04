//! The watcher daemon detects terminal states for opt-in bindings and
//! delivers one-time prompts to the bound agents. It also raises desktop
//! notifications for jobs that newly enter a failed state.
//!
//! Single instance: the heartbeat file holds the owner's PID and is touched
//! by a dedicated thread, so a slow poll cycle never makes a live daemon
//! look dead. A starting daemon claims the file and backs off if another
//! PID owns it.

use super::*;
use crate::bindings::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const DAEMON_ALIVE_SECS: u64 = 20;
const BEAT_SECS: u64 = 3;
const HERDR_DEAD_CYCLE_LIMIT: u32 = 90; // ~30 min at 20 s polling
/// Cycles a vanished job may have no accounting record before the agent is
/// told the final state is unavailable (accounting can lag the queue).
const UNKNOWN_LIMIT: u32 = 6;
/// Failed deliveries (e.g. the agent pane was closed) before dropping.
const DELIVERY_FAIL_LIMIT: u32 = 5;
const MAX_VANISH_LOOKUPS: usize = 5;

fn heartbeat_path() -> PathBuf {
    state_dir().join("watcher.heartbeat")
}

fn heartbeat_pid() -> Option<u32> {
    std::fs::read_to_string(heartbeat_path())
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn heartbeat_age() -> Option<Duration> {
    std::fs::metadata(heartbeat_path())
        .and_then(|md| md.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
}

fn fresh_heartbeat() -> bool {
    heartbeat_age().is_some_and(|age| age < Duration::from_secs(DAEMON_ALIVE_SECS))
}

pub fn cmd_start() {
    if fresh_heartbeat() {
        return; // daemon already running
    }
    let exe = match env::current_exe() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("herdr-slurm-ops: cannot locate own executable: {e}");
            return;
        }
    };
    let mut cmd = new_command(exe);
    cmd.arg("daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        // Detach from the caller's console so closing a herdr pane does not
        // take the daemon down with it.
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    if let Err(e) = cmd.spawn() {
        eprintln!("herdr-slurm-ops: failed to spawn watcher: {e}");
    }
}

/// Stop request: removing the heartbeat makes the daemon exit within one
/// beat interval.
pub fn cmd_stop() {
    let _ = std::fs::remove_file(heartbeat_path());
}

/// Become the one daemon, or report that another live one owns the file.
fn claim_daemon(pid: u32) -> bool {
    if fresh_heartbeat() && heartbeat_pid().is_some_and(|p| p != pid) {
        return false;
    }
    let _ = std::fs::write(heartbeat_path(), pid.to_string());
    // Two daemons started at once both write; the last write wins and the
    // other sees a foreign PID here.
    std::thread::sleep(Duration::from_millis(400));
    heartbeat_pid() == Some(pid)
}

fn beat_loop(pid: u32, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::SeqCst) {
        // Missing file = stop requested; foreign PID = superseded.
        if heartbeat_pid() == Some(pid) {
            let _ = std::fs::write(heartbeat_path(), pid.to_string());
        } else {
            stop.store(true, Ordering::SeqCst);
            return;
        }
        sleep_unless_stopped(&stop, BEAT_SECS);
    }
}

fn sleep_unless_stopped(stop: &AtomicBool, secs: u64) {
    for _ in 0..secs * 4 {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[derive(Default)]
struct WatchState {
    /// (machine, job id) -> state seen last cycle, for failure transitions.
    seen: HashMap<(String, String), String>,
    /// Consecutive cycles a bound job was gone with no accounting record.
    misses: HashMap<(String, String), u32>,
    /// Consecutive failed deliveries to the bound agent.
    fails: HashMap<(String, String), u32>,
}

pub fn cmd_daemon() {
    let _ = std::fs::create_dir_all(state_dir());
    let pid = std::process::id();
    if !claim_daemon(pid) {
        return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        std::thread::spawn(move || beat_loop(pid, stop));
    }
    let mut state = WatchState::default();
    let mut herdr_dead: u32 = 0;
    while !stop.load(Ordering::SeqCst) {
        let probe = vec![herdr_bin(), "workspace".into(), "list".into()];
        if run_capture(&probe).is_some() {
            herdr_dead = 0;
        } else {
            herdr_dead += 1;
            if herdr_dead >= HERDR_DEAD_CYCLE_LIMIT {
                break;
            }
        }
        if let Ok(machines) = load_machines() {
            let results = query_all(&machines, Scope::Mine);
            let bindings = load_bindings();
            notify_failed_transitions(&machines, &results, &bindings, &mut state.seen);
            fire_bindings(&machines, &results, &mut state);
        }
        sleep_unless_stopped(&stop, REFRESH_SECS);
    }
    stop.store(true, Ordering::SeqCst);
    if heartbeat_pid() == Some(pid) {
        let _ = std::fs::remove_file(heartbeat_path());
    }
}

// ------------------------------------------------------ final-state lookup --

#[derive(Debug, PartialEq, Eq)]
enum FinalState {
    Terminal { state: String, exit: String },
    /// Still queued/running (or requeued): not final yet.
    Active(String),
    /// No record found.
    Unknown,
}

fn state_rank(state: &str) -> u8 {
    match state {
        "NODE_FAIL" => 7,
        "OUT_OF_MEMORY" => 6,
        "TIMEOUT" => 5,
        "FAILED" | "BOOT_FAIL" | "DEADLINE" => 4,
        "CANCELLED" => 2,
        "COMPLETED" => 1,
        _ => 0,
    }
}

/// Fold per-task (state, exit) pairs of one job or array into one verdict:
/// any non-terminal task keeps it active; otherwise the worst state wins.
fn aggregate_states(pairs: &[(String, String)]) -> FinalState {
    if pairs.is_empty() {
        return FinalState::Unknown;
    }
    if let Some((state, _)) = pairs.iter().find(|(s, _)| !is_terminal_state(s)) {
        return FinalState::Active(state.clone());
    }
    let (state, exit) = pairs
        .iter()
        .max_by_key(|(s, _)| state_rank(s))
        .expect("non-empty");
    FinalState::Terminal {
        state: state.clone(),
        exit: exit.clone(),
    }
}

/// `sacct -n -P -o State,ExitCode` rows.
fn parse_sacct_final(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| {
            let mut parts = line.split('|');
            let state = parts.next()?.split_whitespace().next()?.to_string();
            let exit = parts.next().unwrap_or("").trim().to_string();
            Some((state, exit))
        })
        .collect()
}

/// `scontrol show job` text (one block per array task).
fn parse_scontrol_final(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for token in text.split_whitespace() {
        if let Some(state) = token.strip_prefix("JobState=") {
            out.push((state.to_string(), String::new()));
        } else if let Some(exit) = token.strip_prefix("ExitCode=") {
            if let Some(last) = out.last_mut() {
                if last.1.is_empty() {
                    last.1 = exit.to_string();
                }
            }
        }
    }
    out
}

/// `qstat -f -x -F json` output.
fn parse_pbs_final(text: &str) -> Vec<(String, String)> {
    let Ok(repaired) = repair_pbs_json(text) else {
        return Vec::new();
    };
    let Ok(root) = serde_json::from_str::<serde_json::Value>(&repaired) else {
        return Vec::new();
    };
    root.get("Jobs")
        .and_then(|j| j.as_object())
        .map(|jobs| {
            jobs.values()
                .filter_map(|j| j.as_object())
                .map(|j| (pbs_job_state(j), pbs_exit_status(j).unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default()
}

/// Ask the scheduler what became of a job that left the queue. Slurm:
/// `sacct`, falling back to `scontrol show job` (recently finished jobs,
/// clusters without accounting). PBS: `qstat -f -x`.
fn final_state(machine: &Machine, job_id: &str) -> FinalState {
    if !valid_job_id(job_id) {
        return FinalState::Unknown;
    }
    let qid = shell_quote(&detail_job_id(job_id));
    match machine.scheduler {
        Scheduler::Slurm => {
            if let Ok(text) = ssh_run(
                machine,
                &format!("sacct -j {qid} -X -n -P -o State,ExitCode -S 1970-01-01"),
            ) {
                let verdict = aggregate_states(&parse_sacct_final(&text));
                if verdict != FinalState::Unknown {
                    return verdict;
                }
            }
            match ssh_run(machine, &format!("scontrol show job {qid}")) {
                Ok(text) => aggregate_states(&parse_scontrol_final(&text)),
                Err(_) => FinalState::Unknown,
            }
        }
        Scheduler::Pbs => match ssh_run(
            machine,
            &format!("{} -f -x -F json {qid}", machine.scheduler_command),
        ) {
            Ok(text) => aggregate_states(&parse_pbs_final(&text)),
            Err(_) => FinalState::Unknown,
        },
    }
}

// ---------------------------------------------------------------- bindings --

#[derive(Debug, PartialEq, Eq)]
enum Decision {
    Wait,
    Deliver { state: String, exit: String },
}

/// Decide whether a bound job is finished. `row_states` are the states of
/// the queue rows the binding covers (empty = job left the queue); `lookup`
/// consults the scheduler and is only called when the queue shows nothing
/// still active.
fn decide(
    row_states: &[String],
    lookup: impl FnOnce() -> FinalState,
    misses: &mut u32,
) -> Decision {
    if row_states.iter().any(|s| !is_terminal_state(s)) {
        *misses = 0;
        return Decision::Wait;
    }
    match lookup() {
        FinalState::Terminal { state, exit } => Decision::Deliver { state, exit },
        FinalState::Active(_) => {
            *misses = 0;
            Decision::Wait
        }
        FinalState::Unknown => {
            if let Some(state) = row_states.first() {
                return Decision::Deliver {
                    state: state.clone(),
                    exit: String::new(),
                };
            }
            *misses += 1;
            if *misses >= UNKNOWN_LIMIT {
                Decision::Deliver {
                    state: "UNKNOWN".into(),
                    exit: String::new(),
                }
            } else {
                Decision::Wait
            }
        }
    }
}

/// The opt-in core: for every binding, detect the bound job's terminal
/// state (queue first, then accounting) and deliver a one-time prompt to
/// the bound agent. A cluster whose queue query failed is skipped for the
/// cycle: an unreachable login node must never look like finished jobs.
fn fire_bindings(
    machines: &[Machine],
    results: &HashMap<String, QueryResult>,
    st: &mut WatchState,
) {
    let bindings = load_bindings();
    st.misses.retain(|k, _| bindings.iter().any(|b| (b.machine.clone(), b.job_id.clone()) == *k));
    st.fails.retain(|k, _| bindings.iter().any(|b| (b.machine.clone(), b.job_id.clone()) == *k));
    let mut done: Vec<(String, String)> = Vec::new();
    for b in &bindings {
        let Some(m) = machines.iter().find(|m| m.name == b.machine) else {
            continue; // cluster removed from config; keep binding
        };
        let Some(Ok(jobs)) = results.get(&m.name) else {
            continue; // query failed: say nothing this cycle
        };
        let row_states: Vec<String> = jobs
            .iter()
            .filter(|j| binding_covers(b, &m.name, &j.id))
            .map(|j| j.state.clone())
            .collect();
        let key = (b.machine.clone(), b.job_id.clone());
        let misses = st.misses.entry(key.clone()).or_insert(0);
        let decision = decide(&row_states, || final_state(m, &b.job_id), misses);
        let Decision::Deliver { state, exit } = decision else {
            continue;
        };
        let msg = terminal_message(&b.machine, &b.job_id, &state, &exit);
        let title = format!("Job {} finished on {}", b.job_id, b.machine);
        if notify_agent(&b.pane_id, &msg) {
            desktop_notify(&title, &state, true);
            done.push(key);
            continue;
        }
        let fails = st.fails.entry(key.clone()).or_insert(0);
        *fails += 1;
        if *fails >= DELIVERY_FAIL_LIMIT {
            desktop_notify(
                &title,
                &format!("{state} - agent pane {} unreachable, binding dropped", b.pane_id),
                true,
            );
            done.push(key);
        }
    }
    if !done.is_empty() {
        update_bindings(|bs| {
            bs.retain(|b| !done.contains(&(b.machine.clone(), b.job_id.clone())));
        });
        for key in &done {
            st.misses.remove(key);
            st.fails.remove(key);
        }
    }
}

// ----------------------------------------------------- failure notification --

fn is_failed_state(state: &str) -> bool {
    matches!(
        state,
        "FAILED" | "TIMEOUT" | "OUT_OF_MEMORY" | "NODE_FAIL" | "BOOT_FAIL" | "DEADLINE"
    )
}

/// Desktop notification for every job that newly enters a failed state:
/// either seen changing state in the queue, or vanished from it (jobs that
/// fail between two polls never show a FAILED row) and then resolved via
/// accounting. Jobs with a binding are skipped: their agent prompt already
/// reports the outcome. PENDING-stuck and COMPLETED stay silent.
fn notify_failed_transitions(
    machines: &[Machine],
    results: &HashMap<String, QueryResult>,
    bindings: &[Binding],
    seen: &mut HashMap<(String, String), String>,
) {
    let mut lookups = 0;
    for m in machines {
        let Some(Ok(jobs)) = results.get(&m.name) else {
            continue; // query failed: keep `seen`, judge next cycle
        };
        let bound = |id: &str| bindings.iter().any(|b| binding_covers(b, &m.name, id));
        let current: HashSet<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
        let gone: Vec<(String, String)> = seen
            .iter()
            .filter(|((machine, id), _)| *machine == m.name && !current.contains(id.as_str()))
            .map(|((_, id), prev)| (id.clone(), prev.clone()))
            .collect();
        for (id, prev) in gone {
            seen.remove(&(m.name.clone(), id.clone()));
            if is_terminal_state(&prev) || bound(&id) || lookups >= MAX_VANISH_LOOKUPS {
                continue;
            }
            lookups += 1;
            if let FinalState::Terminal { state, exit } = final_state(m, &id) {
                if is_failed_state(&state) {
                    desktop_notify(
                        &format!("Job {id} failed on {}", m.name),
                        &format!("{state} (exit {exit})"),
                        false,
                    );
                }
            }
        }
        for j in jobs {
            let key = (m.name.clone(), j.id.clone());
            let prev = seen.insert(key, j.state.clone());
            let changed = prev.as_deref().is_some_and(|p| p != j.state);
            if changed && is_failed_state(&j.state) && !bound(&j.id) {
                desktop_notify(
                    &format!("Job {} failed on {}", j.id, m.name),
                    &format!("{} · {}", j.state, j.reason),
                    false,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect()
    }

    #[test]
    fn running_task_keeps_array_active() {
        let v = aggregate_states(&pairs(&[("COMPLETED", "0:0"), ("RUNNING", "0:0")]));
        assert_eq!(v, FinalState::Active("RUNNING".into()));
    }

    #[test]
    fn worst_state_wins_and_carries_its_exit_code() {
        let v = aggregate_states(&pairs(&[
            ("COMPLETED", "0:0"),
            ("TIMEOUT", "0:15"),
            ("FAILED", "1:0"),
        ]));
        assert_eq!(
            v,
            FinalState::Terminal {
                state: "TIMEOUT".into(),
                exit: "0:15".into()
            }
        );
        assert_eq!(aggregate_states(&[]), FinalState::Unknown);
    }

    #[test]
    fn requeued_job_is_not_final() {
        let v = aggregate_states(&pairs(&[("REQUEUED", "0:0")]));
        assert!(matches!(v, FinalState::Active(_)));
    }

    #[test]
    fn sacct_and_scontrol_outputs_parse() {
        let sacct = "CANCELLED by 1000|0:15\nCOMPLETED|0:0\n\n";
        assert_eq!(
            parse_sacct_final(sacct),
            pairs(&[("CANCELLED", "0:15"), ("COMPLETED", "0:0")])
        );
        let scontrol = "JobId=7 JobName=x\n   JobState=FAILED Reason=NonZeroExitCode Dependency=(null)\n   ExitCode=2:0\nJobId=8\n   JobState=COMPLETED Reason=None\n   ExitCode=0:0\n";
        assert_eq!(
            parse_scontrol_final(scontrol),
            pairs(&[("FAILED", "2:0"), ("COMPLETED", "0:0")])
        );
    }

    #[test]
    fn pbs_history_record_resolves_exit() {
        let text = r#"{"Jobs": {"1.srv": {"job_state": "F", "Exit_status": 1},
                                "2.srv": {"job_state": "F", "Exit_status": 0}}}"#;
        let mut got = parse_pbs_final(text);
        got.sort();
        assert_eq!(got, pairs(&[("COMPLETED", "0"), ("FAILED", "1")]));
    }

    #[test]
    fn active_queue_row_never_consults_the_scheduler() {
        let mut misses = 3;
        let d = decide(
            &["RUNNING".to_string()],
            || panic!("must not query"),
            &mut misses,
        );
        assert_eq!(d, Decision::Wait);
        assert_eq!(misses, 0);
    }

    #[test]
    fn vanished_job_with_unknown_state_waits_then_gives_up() {
        let mut misses = 0;
        for _ in 0..UNKNOWN_LIMIT - 1 {
            assert_eq!(decide(&[], || FinalState::Unknown, &mut misses), Decision::Wait);
        }
        assert_eq!(
            decide(&[], || FinalState::Unknown, &mut misses),
            Decision::Deliver {
                state: "UNKNOWN".into(),
                exit: String::new()
            }
        );
    }

    #[test]
    fn vanished_job_still_active_in_accounting_waits() {
        let mut misses = 0;
        let d = decide(&[], || FinalState::Active("PENDING".into()), &mut misses);
        assert_eq!(d, Decision::Wait);
    }

    #[test]
    fn vanished_job_delivers_terminal_state_with_exit() {
        let mut misses = 0;
        let d = decide(
            &[],
            || FinalState::Terminal {
                state: "FAILED".into(),
                exit: "1:0".into(),
            },
            &mut misses,
        );
        assert_eq!(
            d,
            Decision::Deliver {
                state: "FAILED".into(),
                exit: "1:0".into()
            }
        );
    }
}
