#!/usr/bin/env python3
"""Run a queued Monte Carlo pi computation on Hetzner Cloud.

Flow:
  1. terraform apply (idempotent -- existing servers are reused)
  2. wait for cloud-init to finish on every host
  3. push the current git HEAD to every host over ssh
  4. start the work queue server on host 0 (the coordinator) as a detached
     systemd unit, then a worker unit on every host; the coordinator's worker
     reserves one CPU for the queue server
  5. poll the queue and the units, reporting crashes and failures; a dead
     worker's leased tasks expire and are picked up by the remaining workers
  6. download the queue state, collate the pi estimate, terraform destroy

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
CLOUD_INIT_TIMEOUT = 1200
UNREACHABLE_LIMIT = 30  # consecutive failed polls before a host counts as failed
MISSING_LIMIT = 3  # consecutive polls with no worker unit before failing

DEFAULT_SSH_KEY_PATH = "~/.ssh/id_ed25519"

# Hosts are ephemeral and their IPs get recycled, so skip host key checking.
# Extended with the identity file in main() once args are parsed.
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "ConnectTimeout=10",
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


# ---------------------------------------------------------------- terraform

def terraform(*args: str, capture: bool = False):
    cmd = ["terraform", f"-chdir={INFRA_DIR}", *args]
    if capture:
        return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    subprocess.run(cmd, check=True)


def ensure_init() -> None:
    if not (INFRA_DIR / ".terraform").exists():
        terraform("init", "-input=false")


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

def ssh_run(host, port, remote_cmd, check=True):
    cmd = ["ssh", "-p", str(port), *SSH_OPTS, f"{SSH_USER}@{host['ipv4']}", remote_cmd]
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def wait_cloud_init(host, port) -> None:
    deadline = time.time() + CLOUD_INIT_TIMEOUT
    while True:
        # Blocks until cloud-init finishes; 2 = done with recoverable errors,
        # 255 = ssh itself failed (host still booting / sshd not reconfigured yet).
        r = ssh_run(host, port, "cloud-init status --wait", check=False)
        if r.returncode in (0, 2):
            return
        if r.returncode != 255:
            raise RuntimeError(f"cloud-init failed on {host['name']}: {r.stdout}{r.stderr}")
        if "Permission denied" in r.stderr:
            # Auth failures never self-heal; don't sit in the retry loop.
            raise RuntimeError(
                f"ssh authentication to {host['name']} failed ({r.stderr.strip()}). "
                f"Pass the private key matching ssh_public_key in terraform.tfvars "
                f"via --ssh-key, or load it into ssh-agent."
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
    r = ssh_run(coord, port, COORD_PROBE, check=False)
    if r.returncode != 0:
        return None
    parts = (r.stdout.split("@@@") + ["", "", ""])[:3]
    return parse_unit(parts[0]), parse_unit(parts[1]), parse_json(parts[2])


def probe_worker(host, port):
    """Return the worker unit properties, or None if unreachable."""
    r = ssh_run(host, port, WORKER_PROBE, check=False)
    return parse_unit(r.stdout) if r.returncode == 0 else None


def poll_until_done(hosts, port) -> None:
    """Poll until every task is done. Raises RuntimeError on unrecoverable failure."""
    coord = hosts[0]
    strikes = Counter()
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
            # Queue not answering: crashed unit is fatal, otherwise give it time.
            if queue_unit.get("ActiveState") not in ("active", "activating"):
                raise RuntimeError(
                    f"queue server on {coord['name']} is not running "
                    f"(ActiveState={queue_unit.get('ActiveState')}, "
                    f"Result={queue_unit.get('Result')}); "
                    f"see: ssh {coord['ipv4']} journalctl -u {QUEUE_UNIT}"
                )
            strikes["queue"] += 1
            log(f"[queue] not responding yet ({strikes['queue']}/{MISSING_LIMIT})")
            if strikes["queue"] >= MISSING_LIMIT:
                raise RuntimeError("queue server unit is active but not answering")
            time.sleep(POLL_SECONDS)
            continue
        strikes["queue"] = 0

        # Check every host's worker unit; the coordinator's came with the probe.
        units = {0: coord_worker}
        for host in hosts[1:]:
            if host["index"] not in dead:
                units[host["index"]] = probe_worker(host, port)
        active = 0
        for host in hosts:
            i = host["index"]
            if i in dead:
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
                mark_dead(host, f"unit failed (Result={unit.get('Result')})")
            else:
                # Transient units vanish once they stop; before the queue is
                # done that means the worker exited early (crash/OOM-kill).
                strikes[i] += 1
                if strikes[i] >= MISSING_LIMIT:
                    mark_dead(host, "worker process exited before the queue was done")
        if active == 0:
            raise RuntimeError(
                "no workers left alive and the queue is not done; "
                "rerun to restart the workers, or inspect the hosts"
            )

        log(f"[queue] {status['done']}/{status['num_tasks']} tasks done, "
            f"{status['leased']} leased, {status['pending']} pending; "
            f"{active}/{len(hosts)} workers active")
        time.sleep(POLL_SECONDS)


# ------------------------------------------------------------ results

def download_state(coord, port) -> dict:
    r = ssh_run(coord, port, f"cat {REMOTE_QUEUE_STATE}")
    state = parse_json(r.stdout)
    if state is None:
        raise RuntimeError(f"invalid queue state from {coord['name']}")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    FINAL_STATE.write_text(json.dumps(state, indent=2))
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
    (STATE_DIR / "results.json").write_text(json.dumps(out, indent=2))
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
    ap.add_argument(
        "--ssh-key", default=DEFAULT_SSH_KEY_PATH,
        help="private key matching ssh_public_key in terraform.tfvars "
             "(if missing, falls back to ssh-agent/default keys)",
    )
    args = ap.parse_args()

    ssh_key = Path(args.ssh_key).expanduser()
    if ssh_key.exists():
        SSH_OPTS.extend(["-o", f"IdentityFile={ssh_key}", "-o", "IdentitiesOnly=yes"])
    else:
        log(f"note: ssh key {ssh_key} not found; relying on ssh-agent/default keys")

    # Fast path: previous run already downloaded the finished queue state
    # (e.g. it was interrupted between download and destroy).
    if FINAL_STATE.exists():
        log("Queue results already downloaded")
        collate(json.loads(FINAL_STATE.read_text()))
        if not args.keep_infra:
            maybe_destroy()
        return

    num_tasks = math.ceil(args.total_samples / args.task_samples)

    warn_dirty()
    log("Applying terraform...")
    hosts, port = tf_apply()
    coord = hosts[0]
    log(f"{len(hosts)} host(s): " + ", ".join(
        f"{h['name']}={h['ipv4']}" + (" (coordinator)" if h["index"] == 0 else "")
        for h in hosts))

    with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
        list(pool.map(lambda h: prepare(h, port), hosts))

    # Queue first so workers have something to connect to (they retry anyway).
    start_queue(coord, port, num_tasks, args.task_samples)
    queue_url = f"http://{coord['private_ip']}:{QUEUE_PORT}"
    for host in hosts:
        start_worker(host, port, queue_url)

    try:
        poll_until_done(hosts, port)
    except RuntimeError as e:
        log(f"FAILED: {e}")
        log(f"Leaving servers up for inspection; rerun to retry, "
            f"or clean up with: terraform -chdir={INFRA_DIR} destroy")
        sys.exit(1)

    collate(download_state(coord, port))
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
