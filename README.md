# herdr-slurm-ops

Interactive Slurm/PBS **job control** for [herdr](https://herdr.dev): inspect,
follow logs, hold/release/requeue/cancel, change time limits — plus **opt-in,
per-job agent notification** when a job finishes. Companion to the read-only
[`ivan.herdr-slurm`](../herdr-slurm) monitor plugin; it does not duplicate it.

## Keys

| Key | Action |
|---|---|
| `j`/`k`, arrows, `PgUp`/`PgDn`, `g`/`G` | move selection (follows the same job across refreshes) |
| `Space` | mark/unmark a job; `h u e x t` then act on **all marked** jobs |
| `Enter` | detail: `scontrol show job` / `qstat -f` + `sinfo` summary (finished jobs: `sacct`); scrollable |
| `o` | follow the job log (`tail -F`), scrollable, `G` resumes following; `q`/`Esc` back |
| `n` | pick an agent pane (`j/k`, `1-9`, `Enter`); `n` again on a bound job detaches |
| `h` / `u` | hold / release |
| `e` | requeue (two presses) |
| `x` | cancel (two presses; names the job count and ids) |
| `t` | set time limit (`scontrol update … TimeLimit=` / `qalter -l walltime=`) |
| `/` | filter by id/name/state/account/node |
| `s` | cycle sort: queue / state / name / elapsed |
| `a` | all-users queue (`a` again returns) |
| `H` | last-24h history from `sacct` / `qstat -x` (`H` again returns); read-only |
| `r` | refresh now |
| `Esc` | clear marks, then filter |
| `?` | help; `q` quit |

Destructive actions (`x`, `e`) need the identical key pressed twice on the same
job set; any other key disarms. Nothing runs until the second press. Messages
last until the next key. In raw mode `Ctrl+C` only leaves a sub-view; it never
closes the pane.

All network work (queries, actions, detail, log follow) runs on background
threads, so a slow login node never delays a keypress. Rows are only the
current user's jobs unless `a` is used.

## Notifications

- **Opt-in, per job**: `n` binds a job to a live agent pane. When it reaches a
  terminal state (COMPLETED/FAILED/CANCELLED/TIMEOUT/OUT_OF_MEMORY/NODE_FAIL/
  BOOT_FAIL/DEADLINE) the watcher sends **one** prompt, e.g.
  `[scheduler] job 12345 on MyCluster finished: FAILED (exit 1:0).`, then
  drops the binding. Bound jobs are marked `*`.
- Binding an array row (`123_[4-8%2]`) binds the **whole array**; the prompt
  is sent when no task is left and reports the worst task state.
- A job that leaves the queue is resolved via `sacct`, falling back to
  `scontrol show job` (clusters without accounting) or `qstat -f -x` (PBS,
  where a finished job with non-zero `Exit_status` is reported FAILED). A job
  accounting still reports as active (requeued, array tasks pending) is **not**
  reported. If no record exists for ~2 minutes, the agent is told the final
  state is unavailable.
- An unreachable cluster never fires bindings: a failed queue query is skipped
  for that cycle.
- If the bound agent pane is gone, delivery is retried 5 cycles, then a
  desktop notification says so and the binding is dropped.
- Desktop notifications for jobs that enter FAILED/TIMEOUT/OUT_OF_MEMORY/
  NODE_FAIL/BOOT_FAIL/DEADLINE — including jobs that fail between two polls and
  never show a failed row (resolved through accounting). Bound jobs are not
  announced twice.
- Bindings survive restarts; they are stored atomically under a lock so the
  pane and the daemon cannot overwrite each other.

## Install / link

```sh
cargo build --release
herdr plugin link /path/to/herdr-slurm-ops
herdr plugin pane open --plugin ivan.herdr-slurm-ops --entrypoint ops-windows
```

On linux/macos the entrypoint is `ops`. `herdr plugin install` from GitHub
runs `cargo build --release` automatically via the manifest `[[build]]` step.
Stop the watcher (`herdr-slurm-ops stop`) before rebuilding on Windows, since a
running daemon locks the executable.

## Configuration

Cluster config is **read from the monitor plugin's `machines.toml`**
(`herdr plugin config-dir ivan.herdr-slurm`). Lookup order:
`HERDR_SLURM_MACHINES_DIR`, then `herdr plugin config-dir ivan.herdr-slurm`,
then herdr's default plugin config layout. A missing or invalid file is shown
in the pane, not swallowed.

SSH must work non-interactively (`BatchMode=yes`). `HERDR_SLURM_OPS_SSH`
overrides the `ssh` executable. Job ids are validated before being used in any
remote command and are single-quoted remotely; log paths and time limits are
quoted/validated as well.

## Actions

| Action | What it does |
|---|---|
| `ivan.herdr-slurm-ops.open` | Open the Scheduler Ops pane (resized to 50 cols) |
| `ivan.herdr-slurm-ops.refresh` | One-shot scheduler snapshot, printed to stdout |
| `ivan.herdr-slurm-ops.stop` | Stop the watcher daemon |

(`…-windows` variants on Windows.) Example keybinding:

```toml
[[keys.command]]
key = "prefix+shift+m"
type = "plugin_action"
command = "ivan.herdr-slurm-ops.open-windows" # use ...open on linux/macos
description = "Scheduler job control"
```

## Watcher daemon

Started by the `[[startup]]` hook and by every ops pane. Polls every 20 s. It
is a **single instance**: the heartbeat file holds its PID and is refreshed by
a dedicated thread; a second daemon backs off. `herdr-slurm-ops stop` ends it
within a few seconds. It exits after ~30 min without a reachable herdr server.

## Notes

- Slurm: `squeue`, `scontrol`, `scancel`, `sacct`, `sinfo`. PBS: `qstat -f -F
  json`, `qdel`, `qhold`, `qrls`, `qrerun`, `qalter`, `qstat -f -x`.
- Log follow uses `tail -F` through a remote wrapper that stops `tail` when
  the ssh connection closes, so no orphaned `tail` is left on the login node.
  Array master rows whose log path contains `%a` ask you to pick an element.
  ANSI escapes and control characters are stripped; progress bars show their
  last frame.
- History view lists the last 24 h; hold/cancel/etc. are disabled there.
- `terminal resize` is handled; the layout targets the 50-column sidebar.
- Requirements: Rust toolchain, OpenSSH client on PATH, scheduler executables
  on each cluster.
