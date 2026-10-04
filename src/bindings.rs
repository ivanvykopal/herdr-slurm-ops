//! Opt-in, per-job notification bindings. Nothing notifies anything until
//! the user presses `n` in the ops pane and picks an agent for a specific
//! job. Bindings live in the plugin state dir and survive daemon restarts.
//!
//! The pane and the watcher daemon both mutate the file, so every change
//! goes through [`update_bindings`]: a lock file serializes writers and the
//! file is replaced atomically (write temp + rename).

use super::*;
use std::fs::OpenOptions;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub machine: String,
    pub job_id: String,
    pub pane_id: String,
    pub agent: String,
}

pub fn bindings_path() -> PathBuf {
    state_dir().join("bindings.json")
}

pub fn load_bindings() -> Vec<Binding> {
    let Ok(raw) = std::fs::read_to_string(bindings_path()) else {
        return Vec::new();
    };
    parse_bindings(&raw)
}

pub fn parse_bindings(raw: &str) -> Vec<Binding> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(list) = v.get("bindings").and_then(|b| b.as_array()) {
        for b in list {
            let s = |k: &str| b.get(k).and_then(|x| x.as_str()).unwrap_or("");
            if !s("machine").is_empty() && !s("job_id").is_empty() && !s("pane_id").is_empty() {
                out.push(Binding {
                    machine: s("machine").into(),
                    job_id: s("job_id").into(),
                    pane_id: s("pane_id").into(),
                    agent: s("agent").into(),
                });
            }
        }
    }
    out
}

fn serialize_bindings(bindings: &[Binding]) -> String {
    let arr: Vec<_> = bindings
        .iter()
        .map(|b| {
            serde_json::json!({
                "machine": b.machine,
                "job_id": b.job_id,
                "pane_id": b.pane_id,
                "agent": b.agent,
            })
        })
        .collect();
    serde_json::json!({ "bindings": arr }).to_string()
}

struct LockGuard(PathBuf);

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

const LOCK_STALE: Duration = Duration::from_secs(10);

/// Exclusive lock via `create_new`; a lock older than `LOCK_STALE` belongs
/// to a crashed holder and is broken.
fn acquire_lock() -> Option<LockGuard> {
    let path = state_dir().join("bindings.lock");
    for _ in 0..150 {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => return Some(LockGuard(path)),
            Err(_) => {
                let stale = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .is_some_and(|age| age > LOCK_STALE);
                if stale {
                    let _ = std::fs::remove_file(&path);
                } else {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }
    None
}

/// Read-modify-write under the lock; returns whatever `f` returns.
pub fn update_bindings<R>(f: impl FnOnce(&mut Vec<Binding>) -> R) -> R {
    let _ = std::fs::create_dir_all(state_dir());
    let _lock = acquire_lock();
    let mut bindings = load_bindings();
    let result = f(&mut bindings);
    let path = bindings_path();
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, serialize_bindings(&bindings)).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
    result
}

pub fn binding_matches(b: &Binding, machine: &str, job_id: &str) -> bool {
    b.machine == machine && b.job_id == job_id
}

/// The id a binding is stored under: array range rows (`123_[4-8%2]`) bind
/// to the whole array (`123`); plain and element ids are kept.
pub fn binding_key(row_id: &str) -> String {
    detail_job_id(row_id)
}

/// Does binding `b` cover the queue row `row_id`? A whole-array binding
/// (`123`) covers every row of that array (`123_4`, `123_[5-9]`).
pub fn binding_covers(b: &Binding, machine: &str, row_id: &str) -> bool {
    b.machine == machine
        && (b.job_id == row_id
            || (!b.job_id.contains('_') && row_id.split('_').next() == Some(b.job_id.as_str())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_array_binding_covers_its_rows_only() {
        let b = Binding {
            machine: "C".into(),
            job_id: "123".into(),
            pane_id: "p".into(),
            agent: "a".into(),
        };
        assert!(binding_covers(&b, "C", "123"));
        assert!(binding_covers(&b, "C", "123_4"));
        assert!(binding_covers(&b, "C", "123_[5-9%2]"));
        assert!(!binding_covers(&b, "C", "1234"));
        assert!(!binding_covers(&b, "D", "123"));
        let element = Binding {
            job_id: "123_4".into(),
            ..b
        };
        assert!(binding_covers(&element, "C", "123_4"));
        assert!(!binding_covers(&element, "C", "123_5"));
    }

    #[test]
    fn concurrent_updates_are_not_lost() {
        let dir = env::temp_dir().join(format!("herdr-slurm-ops-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        env::set_var("HERDR_PLUGIN_STATE_DIR", &dir);
        let handles: Vec<_> = (0..16)
            .map(|i| {
                std::thread::spawn(move || {
                    update_bindings(|bs| {
                        bs.push(Binding {
                            machine: "C".into(),
                            job_id: i.to_string(),
                            pane_id: "p".into(),
                            agent: "a".into(),
                        })
                    })
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(load_bindings().len(), 16);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
