#!/usr/bin/env python3
"""Run a queued Monte Carlo pi computation on Hetzner Cloud.

Flow:
  1. terraform apply (idempotent -- existing servers are reused)
  2. prepare hosts concurrently (cloud-init and push the current Git HEAD)
  3. as soon as host 0 is ready, start its persistent queue and worker;
     every other host starts working as soon as its own setup finishes
  4. poll the queue and worker units concurrently; restart crashed units;
     a permanently dead worker's leases expire and move to other workers
  5. download the queue state, collate the pi estimate, terraform destroy

Resumable: terraform state tracks the servers, the queue server persists its
state to disk on the coordinator, and the downloaded queue state under state/
marks the run as finished. Ctrl-C and rerun at any point to pick up where it
left off. Delete state/ to start a fresh computation (and destroy or reset
the servers, since the remote queue state also persists).
"""

import argparse
import json
import math
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
INFRA_DIR = HERE / "infra"
STATE_DIR = HERE / "state"
FINAL_STATE = STATE_DIR / "queue_final.json"

SSH_USER = "admin"
REMOTE_BARE = "/home/admin/app.git"
REMOTE_TREE = "/home/admin/app"
REMOTE_QUEUE_STATE = "/home/admin/queue_state/queue.json"
QUEUE_UNIT = "work-queue"
WORKER_UNIT = "queue-worker"
QUEUE_PORT = 8080

# ~1 minute across 3 worker vCPUs (2x 2-vCPU hosts, coordinator reserves one).
DEFAULT_TOTAL_SAMPLES = 600_000_000
DEFAULT_TASK_SAMPLES = 10_000_000

POLL_SECONDS = 8
CLOUD_INIT_TIMEOUT = 1800
UNREACHABLE_LIMIT = 30  # consecutive failed polls before a host counts as failed
MISSING_LIMIT = 3  # consecutive polls with no worker unit before failing
MAX_RESTARTS = 3
SSH_TIMEOUT = 300
PROBE_PARALLEL = 16
SETUP_PARALLEL = 16

# Hosts are ephemeral and their IPs get recycled, so skip host key checking.
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=4",
]

COORD_PROBE = (
    f"systemctl show {QUEUE_UNIT}.service --property=ActiveState,Result 2>/dev/null; "
    f"echo @@@; "
    f"systemctl show {WORKER_UNIT}.service --property=ActiveState,Result 2>/dev/null; "
    f"echo @@@; curl -sf -m 5 http://localhost:{QUEUE_PORT}/status; echo"
)
WORKER_PROBE = f"systemctl show {WORKER_UNIT}.service --property=ActiveState,Result 2>/dev/null"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def atomic_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    os.replace(tmp, path)


# ---------------------------------------------------------------- terraform

def terraform(*args: str, capture: bool = False):
    cmd = ["terraform", f"-chdir={INFRA_DIR}", *args]
    if capture:
        return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    subprocess.run(cmd, check=True)


def ensure_init() -> None:
    # .terraform/terraform.tfstate holds the backend config; a bare .terraform/
    # (e.g. from `init -backend=false`) still needs a real init.
    if not (INFRA_DIR / ".terraform" / "terraform.tfstate").exists():
        terraform("init", "-reconfigure", "-input=false")


def tf_apply():
    ensure_init()
    terraform("apply", "-auto-approve", "-input=false")
    out = json.loads(terraform("output", "-json", capture=True))
    return out["hosts"]["value"], out["ssh_port"]["value"]


def maybe_destroy() -> None:
    if not (INFRA_DIR / "terraform.tfstate").exists():
        return
    ensure_init()
    terraform("destroy", "-auto-approve", "-input=false")


# ---------------------------------------------------------------------- ssh

def ssh_run(host, port, remote_cmd, check=True, timeout=SSH_TIMEOUT):
    cmd = ["ssh", "-p", str(port), *SSH_OPTS, f"{SSH_USER}@{host['ipv4']}", remote_cmd]
    try:
        return subprocess.run(
            cmd, check=check, capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        error = f"ssh timed out after {timeout}s"
        if check:
            raise subprocess.CalledProcessError(255, cmd, "", error)
        return subprocess.CompletedProcess(cmd, 255, "", error)


def wait_cloud_init(host, port) -> None:
    deadline = time.time() + CLOUD_INIT_TIMEOUT
    while True:
        # Blocks until cloud-init finishes; 2 = done with recoverable errors,
        # 255 = ssh itself failed (host still booting / sshd not reconfigured yet).
        r = ssh_run(
            host, port, "cloud-init status --wait", check=False,
            timeout=CLOUD_INIT_TIMEOUT,
        )
        if r.returncode in (0, 2):
            return
        if r.returncode != 255:
            raise RuntimeError(f"cloud-init failed on {host['name']}: {r.stdout}{r.stderr}")
        if "Permission denied" in r.stderr:
            # Auth failures never self-heal; don't sit in the retry loop.
            raise RuntimeError(
                f"ssh authentication to {host['name']} failed ({r.stderr.strip()}). "
                f"Make sure the private key matching ssh_public_key in "
                f"terraform.tfvars is available to ssh (ssh-agent or ~/.ssh)."
            )
        if time.time() > deadline:
            raise RuntimeError(f"timed out waiting for ssh/cloud-init on {host['name']}")
        time.sleep(10)


# ------------------------------------------------------------------ deploy

def push_code(host, port) -> None:
    ssh_run(host, port, f"git init -q --bare {REMOTE_BARE}")
    env = {**os.environ, "GIT_SSH_COMMAND": "ssh " + " ".join(SSH_OPTS)}
    subprocess.run(
        [
            "git", "push", "--force", "--quiet",
            f"ssh://{SSH_USER}@{host['ipv4']}:{port}{REMOTE_BARE}",
            "HEAD:refs/heads/main",
        ],
        cwd=HERE, env=env, check=True, capture_output=True, text=True,
        timeout=SSH_TIMEOUT,
    )
    ssh_run(
        host, port,
        f"mkdir -p {REMOTE_TREE} && "
        f"git --git-dir={REMOTE_BARE} --work-tree={REMOTE_TREE} checkout -qf main",
    )


def unit_active(host, port, unit) -> bool:
    r = ssh_run(host, port, f"systemctl is-active {unit}.service", check=False)
    return r.stdout.strip() in ("active", "activating")


def start_unit(host, port, unit, command) -> None:
    """Start a detached transient systemd unit (survives with no ssh open)."""
    ssh_run(
        host, port,
        f"sudo systemctl reset-failed {unit}.service 2>/dev/null; "
        f"sudo systemd-run --quiet --unit={unit} --uid={SSH_USER} --gid={SSH_USER} "
        f"--working-directory={REMOTE_TREE}/queued_example {command}",
    )


def start_queue(coord, port, num_tasks, task_samples) -> None:
    if unit_active(coord, port, QUEUE_UNIT):
        log(f"[{coord['name']}] queue server already running")
        return
    start_unit(
        coord, port, QUEUE_UNIT,
        f"/usr/bin/python3 queue_server.py --port {QUEUE_PORT}"
        f" --state-file {REMOTE_QUEUE_STATE}"
        f" --num-tasks {num_tasks} --samples-per-task {task_samples}",
    )
    log(f"[{coord['name']}] queue server started ({num_tasks} tasks x {task_samples:,} samples)")


def restart_queue(coord, port, num_tasks, task_samples) -> None:
    ssh_run(coord, port, f"sudo systemctl stop {QUEUE_UNIT}.service 2>/dev/null", check=False)
    start_queue(coord, port, num_tasks, task_samples)


def start_worker(host, port, queue_url) -> None:
    if unit_active(host, port, WORKER_UNIT):
        log(f"[{host['name']}] worker already running")
        return
    reserve = 1 if host["index"] == 0 else 0
    start_unit(
        host, port, WORKER_UNIT,
        f"/usr/bin/python3 worker.py --queue-url {queue_url} --reserve-cpus {reserve}",
    )
    log(f"[{host['name']}] worker started" + (" (1 CPU reserved for queue)" if reserve else ""))


# ----------------------------------------------------------------- monitor

def parse_json(text: str):
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def parse_unit(text: str) -> dict:
    return dict(l.split("=", 1) for l in text.strip().splitlines() if "=" in l)


def probe_coordinator(coord, port):
    """Return (queue_unit, worker_unit, queue_status) or None if unreachable."""
    try:
        r = ssh_run(coord, port, COORD_PROBE, check=False)
    except OSError:
        return None
    if r.returncode != 0:
        return None
    parts = (r.stdout.split("@@@") + ["", "", ""])[:3]
    return parse_unit(parts[0]), parse_unit(parts[1]), parse_json(parts[2])


def probe_worker(host, port):
    """Return the worker unit properties, or None if unreachable."""
    try:
        r = ssh_run(host, port, WORKER_PROBE, check=False)
    except OSError:
        return None
    return parse_unit(r.stdout) if r.returncode == 0 else None


def poll_until_done(hosts, port, queue_url, num_tasks, task_samples) -> None:
    """Poll until every task is done. Raises RuntimeError on unrecoverable failure."""
    coord = hosts[0]
    strikes = Counter()
    restarts = Counter()
    dead = set()  # host indices whose worker is gone (queue can still finish)

    def mark_dead(host, reason):
        if host["index"] not in dead:
            dead.add(host["index"])
            log(f"[{host['name']}] worker FAILED: {reason} "
                f"(its leased tasks will be re-queued)")

    while True:
        probed = probe_coordinator(coord, port)
        if probed is None:
            strikes["coord"] += 1
            log(f"[{coord['name']}] unreachable ({strikes['coord']}/{UNREACHABLE_LIMIT})")
            if strikes["coord"] >= UNREACHABLE_LIMIT:
                raise RuntimeError(f"coordinator {coord['name']} unreachable, giving up")
            time.sleep(POLL_SECONDS)
            continue
        strikes["coord"] = 0
        queue_unit, coord_worker, status = probed

        if status and status["done"] >= status["num_tasks"]:
            log(f"[queue] all {status['num_tasks']} tasks done")
            return

        if status is None:
            strikes["queue"] += 1
            log(f"[queue] not responding yet ({strikes['queue']}/{MISSING_LIMIT})")
            inactive = queue_unit.get("ActiveState") not in ("active", "activating")
            if inactive or strikes["queue"] >= MISSING_LIMIT:
                if restarts["queue"] >= MAX_RESTARTS:
                    raise RuntimeError(
                        f"queue server failed after {MAX_RESTARTS} restarts "
                        f"(ActiveState={queue_unit.get('ActiveState')}, "
                        f"Result={queue_unit.get('Result')})"
                    )
                restarts["queue"] += 1
                log(
                    f"[queue] restarting persistent server "
                    f"({restarts['queue']}/{MAX_RESTARTS})"
                )
                try:
                    restart_queue(coord, port, num_tasks, task_samples)
                except (subprocess.CalledProcessError, OSError) as error:
                    raise RuntimeError(
                        f"could not restart queue server: {error}"
                    ) from error
                strikes["queue"] = 0
            time.sleep(POLL_SECONDS)
            continue
        strikes["queue"] = 0

        # Check ready workers concurrently; one slow SSH connection must not
        # delay dispatch/health decisions for the entire fleet.
        units = {0: coord_worker}
        to_probe = [
            host for host in hosts[1:]
            if host["index"] not in dead and host.get("worker_started")
        ]
        if to_probe:
            with ThreadPoolExecutor(
                    max_workers=min(PROBE_PARALLEL, len(to_probe))) as pool:
                probed_workers = list(pool.map(
                    lambda host: probe_worker(host, port), to_probe,
                ))
            units.update({
                host["index"]: unit
                for host, unit in zip(to_probe, probed_workers)
            })
        active = 0
        pending_setup = 0
        for host in hosts:
            i = host["index"]
            if i in dead:
                continue
            if not host.get("worker_started"):
                if host.get("setup_error"):
                    mark_dead(host, f"setup failed: {host['setup_error']}")
                else:
                    pending_setup += 1
                continue
            unit = units.get(i)
            if unit is None:
                strikes[i] += 1
                log(f"[{host['name']}] unreachable ({strikes[i]}/{UNREACHABLE_LIMIT})")
                if strikes[i] >= UNREACHABLE_LIMIT:
                    mark_dead(host, "host unreachable")
                continue
            state = unit.get("ActiveState")
            if state in ("active", "activating"):
                strikes[i] = 0
                active += 1
            elif state == "failed":
                reason = f"unit failed (Result={unit.get('Result')})"
                if restarts[i] < MAX_RESTARTS:
                    restarts[i] += 1
                    log(
                        f"[{host['name']}] {reason}; restarting "
                        f"({restarts[i]}/{MAX_RESTARTS})"
                    )
                    try:
                        start_worker(host, port, queue_url)
                        active += 1
                        strikes[i] = 0
                    except (subprocess.CalledProcessError, OSError) as error:
                        mark_dead(host, f"restart failed: {error}")
                else:
                    mark_dead(host, reason)
            else:
                # Transient units vanish once they stop; before the queue is
                # done that means the worker exited early (crash/OOM-kill).
                strikes[i] += 1
                if strikes[i] >= MISSING_LIMIT:
                    if restarts[i] < MAX_RESTARTS:
                        restarts[i] += 1
                        log(
                            f"[{host['name']}] worker exited; restarting "
                            f"({restarts[i]}/{MAX_RESTARTS})"
                        )
                        try:
                            start_worker(host, port, queue_url)
                            active += 1
                            strikes[i] = 0
                        except (subprocess.CalledProcessError, OSError) as error:
                            mark_dead(host, f"restart failed: {error}")
                    else:
                        mark_dead(host, "worker exited before the queue was done")
        if active == 0 and pending_setup == 0:
            raise RuntimeError(
                "no workers left alive and the queue is not done; "
                "rerun to restart the workers, or inspect the hosts"
            )

        log(f"[queue] {status['done']}/{status['num_tasks']} tasks done, "
            f"{status['leased']} leased, {status['pending']} pending; "
            f"{active}/{len(hosts)} workers active, {pending_setup} setting up")
        time.sleep(POLL_SECONDS)


# ------------------------------------------------------------ results

def valid_final_state(state, num_tasks, task_samples) -> bool:
    if not isinstance(state, dict) \
            or state.get("num_tasks") != num_tasks \
            or state.get("samples_per_task") != task_samples \
            or not isinstance(state.get("done"), dict) \
            or set(state["done"]) != {str(i) for i in range(num_tasks)}:
        return False
    for result in state["done"].values():
        if not isinstance(result, dict) \
                or result.get("samples") != task_samples \
                or not isinstance(result.get("inside"), int) \
                or not 0 <= result["inside"] <= task_samples:
            return False
    return True


def download_state(coord, port, num_tasks, task_samples) -> dict:
    r = ssh_run(coord, port, f"cat {REMOTE_QUEUE_STATE}")
    state = parse_json(r.stdout)
    if not valid_final_state(state, num_tasks, task_samples):
        raise RuntimeError(
            f"queue state from {coord['name']} is incomplete or belongs "
            "to different --total-samples/--task-samples settings"
        )
    atomic_write_json(FINAL_STATE, state)
    log(f"queue state downloaded to {FINAL_STATE}")
    return state


def collate(state: dict) -> None:
    results = list(state["done"].values())
    samples = sum(r["samples"] for r in results)
    inside = sum(r["inside"] for r in results)
    pi = 4 * inside / samples
    by_worker = Counter(r["worker"] for r in results)
    out = {
        "num_tasks": state["num_tasks"],
        "total_samples": samples,
        "inside": inside,
        "pi_estimate": pi,
        "abs_error": abs(pi - math.pi),
        "tasks_by_worker": dict(by_worker.most_common()),
    }
    atomic_write_json(STATE_DIR / "results.json", out)
    log(f"pi ~= {pi:.8f} (abs error {abs(pi - math.pi):.2e}) "
        f"from {samples:,} samples across {len(results)} tasks")
    for worker, n in by_worker.most_common():
        log(f"  {worker}: {n} tasks")
    log(f"collated result written to {STATE_DIR / 'results.json'}")


# ------------------------------------------------------------ orchestration

def prepare(host, port) -> None:
    log(f"[{host['name']}] waiting for cloud-init...")
    wait_cloud_init(host, port)
    log(f"[{host['name']}] cloud-init done, pushing code")
    push_code(host, port)


def warn_dirty() -> None:
    r = subprocess.run(
        ["git", "status", "--porcelain"], cwd=HERE, capture_output=True, text=True
    )
    if r.stdout.strip():
        log("note: uncommitted changes present; hosts receive the last commit (HEAD), "
            "not the working tree")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--total-samples", type=int, default=DEFAULT_TOTAL_SAMPLES,
        help="total Monte Carlo samples (rounded up to a whole number of tasks)",
    )
    ap.add_argument(
        "--task-samples", type=int, default=DEFAULT_TASK_SAMPLES,
        help="samples per queued task",
    )
    ap.add_argument(
        "--keep-infra", action="store_true",
        help="skip terraform destroy at the end",
    )
    args = ap.parse_args()
    if args.total_samples <= 0:
        ap.error("--total-samples must be positive")
    if args.task_samples <= 0:
        ap.error("--task-samples must be positive")

    num_tasks = math.ceil(args.total_samples / args.task_samples)

    # Fast path: previous run already downloaded the finished queue state
    # (e.g. it was interrupted between download and destroy).
    if FINAL_STATE.exists():
        final_state = parse_json(FINAL_STATE.read_text())
        if not valid_final_state(final_state, num_tasks, args.task_samples):
            log(
                f"FAILED: {FINAL_STATE} does not match this run; move or "
                "remove state/ before changing workload settings"
            )
            raise SystemExit(1)
        log("Queue results already downloaded")
        collate(final_state)
        if not args.keep_infra:
            maybe_destroy()
        return

    warn_dirty()
    log("Applying terraform...")
    hosts, port = tf_apply()
    coord = hosts[0]
    log(f"{len(hosts)} host(s): " + ", ".join(
        f"{h['name']}={h['ipv4']}" + (" (coordinator)" if h["index"] == 0 else "")
        for h in hosts))

    queue_url = f"http://{coord['private_ip']}:{QUEUE_PORT}"
    for host in hosts:
        host["worker_started"] = False
        host["setup_error"] = None

    # Prepare ordinary workers while the coordinator is booting. They wait on
    # this event before starting, so their finite connection retry budget does
    # not expire before the queue exists.
    queue_ready = threading.Event()
    stop_setup = threading.Event()
    setup_slots = threading.Semaphore(SETUP_PARALLEL)

    def prepare_worker(host) -> None:
        try:
            with setup_slots:
                prepare(host, port)
            queue_ready.wait()
            if stop_setup.is_set():
                return
            start_worker(host, port, queue_url)
            host["worker_started"] = True
        except Exception as error:
            host["setup_error"] = str(error)
            log(f"[{host['name']}] setup FAILED: {error}")

    for host in hosts[1:]:
        threading.Thread(
            target=prepare_worker, args=(host,), daemon=True,
        ).start()

    # The queue is the only ordering dependency. Once its host is ready, both
    # its worker and every independently prepared worker can begin immediately.
    try:
        prepare(coord, port)
        start_queue(coord, port, num_tasks, args.task_samples)
        start_worker(coord, port, queue_url)
        coord["worker_started"] = True
    except Exception as error:
        stop_setup.set()
        queue_ready.set()
        log(f"FAILED to start the coordinator: {error}")
        log("Leaving servers up for inspection; rerun to retry setup")
        raise SystemExit(1)
    queue_ready.set()

    try:
        poll_until_done(
            hosts, port, queue_url, num_tasks, args.task_samples,
        )
    except RuntimeError as e:
        stop_setup.set()
        log(f"FAILED: {e}")
        log(f"Leaving servers up for inspection; rerun to retry, "
            f"or clean up with: terraform -chdir={INFRA_DIR} destroy")
        sys.exit(1)
    stop_setup.set()

    try:
        state = download_state(coord, port, num_tasks, args.task_samples)
    except (RuntimeError, subprocess.CalledProcessError, OSError) as error:
        log(f"FAILED to download a verified final queue state: {error}")
        log("Leaving servers up for inspection; rerun to retry the download")
        raise SystemExit(1)
    collate(state)
    if args.keep_infra:
        log("--keep-infra set, skipping destroy")
    else:
        log("Destroying terraform resources...")
        maybe_destroy()
    log("Done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted. State is saved; rerun to resume.", file=sys.stderr)
        sys.exit(130)
