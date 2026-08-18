#!/usr/bin/env python3
"""Run resumable Monte Carlo batches on an elastic Hetzner Cloud fleet.

The global sample range is divided into deterministic batches. Each ready
host receives one detached systemd job at a time and takes another batch when
it finishes, so faster hosts naturally do more work. By default, idle hosts
are removed as soon as the queue is empty.

The run is resumable at every stage: Terraform records the fleet, local
assignments reconnect hosts to in-flight batches, workers checkpoint every
completed chunk, and verified downloads mark batches complete. A failed unit
is restarted in place; an unreachable or repeatedly failing host is removed,
its batch is requeued, and a replacement is provisioned within the --hosts
cap and a finite retry budget.
"""

import argparse
import json
import math
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
INFRA_DIR = HERE / "infra"
STATE_DIR = HERE / "state"
RESULTS_DIR = STATE_DIR / "results"
CHECKPOINT_DIR = STATE_DIR / "checkpoints"
FLEET_FILE = INFRA_DIR / "hosts.auto.tfvars.json"
ASSIGN_FILE = STATE_DIR / "assignments.json"

SSH_USER = "admin"
REMOTE_BARE = "/home/admin/app.git"
REMOTE_TREE = "/home/admin/app"
REMOTE_OUT = "/home/admin/shard_out"
UNIT = "shard-job"

# ~1 minute across 4 vCPUs; tune these together with the fleet size.
DEFAULT_TOTAL_SAMPLES = 800_000_000
DEFAULT_BATCH_SAMPLES = 100_000_000

POLL_SECONDS = 8
CLOUD_INIT_TIMEOUT = 1800
UNREACHABLE_LIMIT = 30
MISSING_LIMIT = 3
MAX_RESTARTS = 3
BATCH_ATTEMPTS_MAX = 3
CHECKPOINT_PULL_EVERY = 8
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
    # ConnectTimeout only covers the handshake. Keepalives also bound a
    # connection that blackholes after SSH has been established.
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=4",
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def batch_key(start: int, end: int) -> str:
    return f"{start}-{end}"


def result_path(start: int, end: int) -> Path:
    return RESULTS_DIR / f"batch-{batch_key(start, end)}.json"


def checkpoint_path(start: int, end: int) -> Path:
    return CHECKPOINT_DIR / f"batch-{batch_key(start, end)}.json"


def atomic_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    os.replace(tmp, path)


# ---------------------------------------------------------------- terraform

def terraform(*args: str, capture: bool = False):
    cmd = ["terraform", f"-chdir={INFRA_DIR}", *args]
    if capture:
        return subprocess.run(
            cmd, check=True, capture_output=True, text=True,
        ).stdout
    subprocess.run(cmd, check=True)


def ensure_init() -> None:
    # A bare .terraform/ can be left by `terraform init -backend=false`.
    if not (INFRA_DIR / ".terraform" / "terraform.tfstate").exists():
        terraform("init", "-reconfigure", "-input=false")


def fleet_ids():
    if not FLEET_FILE.exists():
        return []
    data = json.loads(FLEET_FILE.read_text())
    return [str(host_id) for host_id in data.get("host_ids", [])]


def ensure_fleet(ids):
    """Converge to exactly these stable worker ids and return sorted hosts."""
    ids = {str(host_id) for host_id in ids}
    if any(not host_id.isdigit() for host_id in ids):
        raise ValueError("fleet host ids must be non-negative integers")
    ids = sorted(ids, key=int)
    ensure_init()
    atomic_write_json(FLEET_FILE, {"host_ids": ids})
    terraform("apply", "-auto-approve", "-input=false")
    out = json.loads(terraform("output", "-json", capture=True))
    hosts = sorted(out["hosts"]["value"], key=lambda host: host["index"])
    return hosts, out["ssh_port"]["value"]


def maybe_destroy() -> None:
    if not (INFRA_DIR / "terraform.tfstate").exists():
        return
    ensure_init()
    terraform("destroy", "-auto-approve", "-input=false")
    FLEET_FILE.unlink(missing_ok=True)


# ---------------------------------------------------------------------- ssh

def ssh_run(host, port, remote_cmd, check=True, timeout=SSH_TIMEOUT,
            stdin=None):
    """Run a bounded remote command; report timeouts as unreachable SSH."""
    cmd = [
        "ssh", "-p", str(port), *SSH_OPTS,
        f"{SSH_USER}@{host['ipv4']}", remote_cmd,
    ]
    try:
        return subprocess.run(
            cmd, check=check, capture_output=True, text=True,
            timeout=timeout, input=stdin,
        )
    except subprocess.TimeoutExpired:
        error = f"ssh timed out after {timeout}s"
        if check:
            raise subprocess.CalledProcessError(255, cmd, "", error)
        return subprocess.CompletedProcess(cmd, 255, "", error)


def wait_cloud_init(host, port) -> None:
    deadline = time.time() + CLOUD_INIT_TIMEOUT
    while True:
        # `--wait` may span package upgrades, so it gets the full boot budget.
        result = ssh_run(
            host, port, "cloud-init status --wait", check=False,
            timeout=CLOUD_INIT_TIMEOUT,
        )
        if result.returncode in (0, 2):
            return
        if result.returncode != 255:
            raise RuntimeError(
                f"cloud-init failed on {host['name']}: "
                f"{result.stdout}{result.stderr}"
            )
        if "Permission denied" in result.stderr:
            raise RuntimeError(
                f"ssh authentication to {host['name']} failed "
                f"({result.stderr.strip()}). Make sure the private key "
                f"matching terraform.tfvars' ssh_public_key is available."
            )
        if time.time() > deadline:
            raise RuntimeError(
                f"timed out waiting for ssh/cloud-init on {host['name']}"
            )
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
        f"git --git-dir={REMOTE_BARE} --work-tree={REMOTE_TREE} "
        f"checkout -qf main",
    )


def remote_file(kind: str, start: int, end: int) -> str:
    return f"{REMOTE_OUT}/{kind}-{batch_key(start, end)}.json"


def start_job(host, port, start: int, end: int) -> None:
    key = batch_key(start, end)
    inner = (
        f"/usr/bin/python3 {REMOTE_TREE}/sharded_example/compute.py"
        f" --sample-start {start} --sample-end {end}"
        f" --out-dir {REMOTE_OUT}"
    )
    ssh_run(
        host, port,
        f"sudo systemctl stop {UNIT}.service 2>/dev/null; "
        f"sudo systemctl reset-failed {UNIT}.service 2>/dev/null; "
        f"mkdir -p {REMOTE_OUT} && "
        f"rm -f {REMOTE_OUT}/status-{key}.json "
        f"{REMOTE_OUT}/progress-{key}.json {REMOTE_OUT}/result-{key}.json; "
        f"sudo systemd-run --quiet --unit={UNIT} --uid={SSH_USER} "
        f"--gid={SSH_USER} --working-directory={REMOTE_TREE}/sharded_example "
        f"--property=Restart=no {inner}",
    )


def valid_checkpoint(data, start: int, end: int) -> bool:
    return (
        isinstance(data, dict)
        and data.get("sample_start") == start
        and data.get("sample_end") == end
        and isinstance(data.get("chunk_samples"), int)
        and data["chunk_samples"] > 0
        and isinstance(data.get("chunks"), dict)
    )


def seed_checkpoint(host, port, start: int, end: int) -> None:
    path = checkpoint_path(start, end)
    if not path.exists() or path.stat().st_size == 0:
        return
    data = parse_json(path.read_text())
    if not valid_checkpoint(data, start, end):
        raise RuntimeError(f"invalid staged checkpoint {path}")
    ssh_run(
        host, port,
        f"mkdir -p {REMOTE_OUT} && cat > "
        f"{remote_file('checkpoint', start, end)}",
        stdin=json.dumps(data),
    )
    log(
        f"[{host['name']}] seeded {len(data['chunks'])}-chunk checkpoint "
        f"for batch {batch_key(start, end)}"
    )


# ----------------------------------------------------------------- monitor

def parse_json(text: str):
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def probe(host, port, start: int, end: int) -> dict:
    try:
        result = ssh_run(
            host, port,
            f"systemctl show {UNIT}.service --property=ActiveState,Result "
            f"2>/dev/null; echo @@@; "
            f"cat {remote_file('status', start, end)} 2>/dev/null; "
            f"echo; echo @@@; "
            f"cat {remote_file('progress', start, end)} 2>/dev/null; echo",
            check=False,
        )
    except OSError as error:
        return {"phase": "unreachable", "detail": str(error)[:200]}
    if result.returncode != 0:
        return {
            "phase": "unreachable",
            "detail": result.stderr.strip()[:200],
        }

    parts = (result.stdout.split("@@@") + ["", ""])[:3]
    unit = dict(
        line.split("=", 1) for line in parts[0].strip().splitlines()
        if "=" in line
    )
    status = parse_json(parts[1]) or {}
    progress = parse_json(parts[2])
    active = unit.get("ActiveState", "")
    if status.get("state") == "done":
        return {"phase": "done"}
    if status.get("state") == "failed":
        error = status.get("error") or "worker reported failure"
        return {"phase": "failed", "detail": error.strip().splitlines()[-1]}
    if active in ("active", "activating"):
        return {"phase": "running", "progress": progress}
    if active == "failed":
        return {
            "phase": "failed",
            "detail": f"systemd unit failed (Result={unit.get('Result')})",
        }
    if status.get("state") == "running":
        return {
            "phase": "failed",
            "detail": "worker process died without completing the batch",
        }
    return {"phase": "missing"}


def pull_checkpoint(host, port, start: int, end: int) -> None:
    try:
        result = ssh_run(
            host, port, remote_cmd=f"cat {remote_file('checkpoint', start, end)}",
            check=False,
        )
    except OSError:
        return
    if result.returncode != 0 or not result.stdout:
        return
    remote = parse_json(result.stdout)
    if not valid_checkpoint(remote, start, end):
        return

    path = checkpoint_path(start, end)
    local = parse_json(path.read_text()) if path.exists() else None
    if valid_checkpoint(local, start, end) \
            and len(local["chunks"]) >= len(remote["chunks"]):
        return
    atomic_write_json(path, remote)


def download_batch(host, port, start: int, end: int) -> None:
    result = ssh_run(host, port, f"cat {remote_file('result', start, end)}")
    parsed = parse_json(result.stdout)
    expected_samples = end - start
    if not isinstance(parsed, dict) \
            or parsed.get("sample_start") != start \
            or parsed.get("sample_end") != end \
            or parsed.get("samples") != expected_samples \
            or not isinstance(parsed.get("inside"), int) \
            or not 0 <= parsed["inside"] <= expected_samples:
        raise RuntimeError(
            f"invalid result for batch {batch_key(start, end)} "
            f"from {host['name']}"
        )
    atomic_write_json(result_path(start, end), parsed)


def batch_done_locally(start: int, end: int) -> bool:
    path = result_path(start, end)
    if not path.exists():
        return False
    parsed = parse_json(path.read_text())
    expected_samples = end - start
    return (
        isinstance(parsed, dict)
        and parsed.get("sample_start") == start
        and parsed.get("sample_end") == end
        and parsed.get("samples") == expected_samples
        and isinstance(parsed.get("inside"), int)
        and 0 <= parsed["inside"] <= expected_samples
    )


# ------------------------------------------------------------ orchestration

def make_batches(total_samples: int, batch_samples: int):
    return [
        (start, min(start + batch_samples, total_samples))
        for start in range(0, total_samples, batch_samples)
    ]


def load_assignments():
    if not ASSIGN_FILE.exists():
        return {}
    return {
        str(host_id): tuple(batch)
        for host_id, batch in json.loads(ASSIGN_FILE.read_text()).items()
    }


def save_assignments(hosts) -> None:
    assignments = {
        host["id"]: list(host["batch"])
        for host in hosts
        if host["alive"] and host["batch"] is not None
    }
    atomic_write_json(ASSIGN_FILE, assignments)


def choose_initial_ids(assignments, target: int):
    chosen = []
    for host_id in sorted(assignments, key=int):
        if host_id not in chosen and len(chosen) < target:
            chosen.append(host_id)
    for host_id in sorted(fleet_ids(), key=int):
        if host_id not in chosen and len(chosen) < target:
            chosen.append(host_id)
    candidate = 0
    while len(chosen) < target:
        host_id = str(candidate)
        if host_id not in chosen:
            chosen.append(host_id)
        candidate += 1
    return chosen


def collate(batches, total_samples: int) -> None:
    results = [json.loads(result_path(*batch).read_text()) for batch in batches]
    samples = sum(result["samples"] for result in results)
    inside = sum(result["inside"] for result in results)
    if samples != total_samples:
        raise RuntimeError(
            f"collation found {samples:,} samples; expected {total_samples:,}"
        )
    pi = 4 * inside / samples
    output = {
        "num_batches": len(batches),
        "total_samples": samples,
        "inside": inside,
        "pi_estimate": pi,
        "abs_error": abs(pi - math.pi),
        "batches": results,
    }
    atomic_write_json(STATE_DIR / "collated.json", output)
    log(
        f"pi ~= {pi:.8f} (abs error {abs(pi - math.pi):.2e}) "
        f"from {samples:,} samples across {len(batches)} batches"
    )
    log(f"collated result written to {STATE_DIR / 'collated.json'}")


def warn_dirty() -> None:
    result = subprocess.run(
        ["git", "status", "--porcelain"], cwd=HERE,
        capture_output=True, text=True,
    )
    if result.stdout.strip():
        log(
            "note: uncommitted changes present; hosts receive the last "
            "commit (HEAD), not the working tree"
        )


def run(args) -> None:
    batches = make_batches(args.total_samples, args.batch_samples)
    pending = [batch for batch in batches if not batch_done_locally(*batch)]
    log(
        f"{len(batches)} batches of <={args.batch_samples:,} samples; "
        f"{len(pending)} pending"
    )
    if args.hosts > len(batches):
        log(
            f"note: --hosts {args.hosts} exceeds the {len(batches)} batches; "
            "use a smaller --batch-samples to feed more hosts"
        )

    pending_set = set(pending)
    raw_assignments = load_assignments()
    assignments = {}
    assigned_batches = set()
    for host_id in sorted(raw_assignments, key=int):
        batch = raw_assignments[host_id]
        if batch in pending_set and batch not in assigned_batches:
            assignments[host_id] = batch
            assigned_batches.add(batch)

    target = min(args.hosts, len(pending))
    ids = choose_initial_ids(assignments, target)
    assignments = {
        host_id: batch for host_id, batch in assignments.items()
        if host_id in ids
    }

    if pending:
        log(f"Applying Terraform for {len(ids)} host(s)...")
        hosts, port = ensure_fleet(ids)
        log(", ".join(f"{host['name']}={host['ipv4']}" for host in hosts))
    else:
        hosts = []
        port = None

    for host in hosts:
        host_id = str(host["index"])
        host.update({
            "id": host_id,
            "batch": assignments.get(host_id),
            "strikes": 0,
            "restarts": 0,
            "alive": True,
            "ready": False,
        })
    queued = [batch for batch in pending
              if batch not in {host["batch"] for host in hosts
                               if host["batch"] is not None}]
    failures = {batch: 0 for batch in pending}
    save_assignments(hosts)

    def prepare(host) -> None:
        try:
            log(f"[{host['name']}] waiting for cloud-init...")
            wait_cloud_init(host, port)
            log(f"[{host['name']}] cloud-init done, pushing code")
            push_code(host, port)
        except Exception as error:
            log(f"[{host['name']}] setup FAILED: {error}")
            host["alive"] = False

    # Do not make the fleet wait for its slowest cloud-init/git push. Each
    # host becomes dispatchable as soon as its own setup succeeds.
    setup_slots = threading.Semaphore(SETUP_PARALLEL)

    def prepare_async(host) -> None:
        with setup_slots:
            if host["alive"]:
                prepare(host)
                if host["alive"]:
                    host["ready"] = True

    for host in hosts:
        threading.Thread(
            target=prepare_async, args=(host,), daemon=True,
        ).start()

    kept_failed = []
    replacements_left = 2 * max(args.hosts, 1)
    known_ids = ids + fleet_ids()
    next_id = max((int(host_id) for host_id in known_ids), default=-1) + 1
    polls = 0
    downloaded = len(batches) - len(pending)

    def requeue(host, reason: str) -> None:
        batch = host["batch"]
        host["batch"] = None
        host["alive"] = False
        if batch is not None:
            failures[batch] += 1
            if failures[batch] >= BATCH_ATTEMPTS_MAX:
                raise RuntimeError(
                    f"batch {batch_key(*batch)} failed on "
                    f"{BATCH_ATTEMPTS_MAX} hosts; aborting"
                )
            queued.insert(0, batch)
        log(
            f"[{host['name']}] {reason}; "
            + (f"requeued batch {batch_key(*batch)}"
               if batch is not None else "no batch assigned")
        )
        save_assignments(hosts)

    while True:
        # Ready idle hosts take the next deterministic batch.
        for host in hosts:
            if not (host["alive"] and host["ready"]
                    and host["batch"] is None and queued):
                continue
            batch = queued.pop(0)
            # Persist ownership before the remote side effect. If the local
            # process dies during systemd-run, a rerun will probe this host
            # instead of dispatching duplicate work elsewhere.
            host["batch"] = batch
            save_assignments(hosts)
            try:
                seed_checkpoint(host, port, *batch)
                start_job(host, port, *batch)
            except (subprocess.CalledProcessError, RuntimeError, OSError) as error:
                host["batch"] = None
                queued.insert(0, batch)
                host["strikes"] += 1
                log(
                    f"[{host['name']}] dispatch failed "
                    f"({str(error)[:160]}); will retry"
                )
                if host["strikes"] >= MISSING_LIMIT:
                    host["alive"] = False
                save_assignments(hosts)
                continue
            host["restarts"] = 0
            host["strikes"] = 0
            log(f"[{host['name']}] started batch {batch_key(*batch)}")
        save_assignments(hosts)

        # A setup failure can strand a resumed assignment; put it back first.
        for host in hosts:
            if not host["alive"] and host["batch"] is not None:
                requeue(host, "host died while holding a batch")

        drop = [
            host for host in hosts
            if not host["alive"]
            or (not args.keep_infra and host["batch"] is None and not queued)
        ]
        if drop:
            for host in drop:
                if not host["alive"] and args.keep_failed:
                    kept_failed.append(host["id"])
                    log(f"[{host['name']}] failed; kept up (--keep-failed)")
                else:
                    state = "failed" if not host["alive"] else "idle"
                    log(f"[{host['name']}] {state}; removing from fleet")
            hosts = [host for host in hosts if host not in drop]
            keep_ids = [host["id"] for host in hosts] + kept_failed
            ensure_fleet(keep_ids)

        # Replace failed capacity only when queued work has no idle host.
        if queued:
            idle = sum(
                1 for host in hosts
                if host["alive"] and host["batch"] is None
            )
            needed = min(
                len(queued) - idle,
                args.hosts - len(hosts),
                replacements_left,
            )
            if needed > 0:
                new_ids = [str(next_id + offset) for offset in range(needed)]
                next_id += needed
                replacements_left -= needed
                log(f"spawning replacement host(s): {', '.join(new_ids)}")
                all_hosts, port = ensure_fleet(
                    [host["id"] for host in hosts] + kept_failed + new_ids
                )
                fresh = [
                    dict(
                        host, id=str(host["index"]), batch=None,
                        strikes=0, restarts=0, alive=True, ready=False,
                    )
                    for host in all_hosts
                    if str(host["index"]) in new_ids
                ]
                hosts.extend(fresh)
                for host in fresh:
                    threading.Thread(
                        target=prepare_async, args=(host,), daemon=True,
                    ).start()

        running = any(host["alive"] and host["batch"] is not None
                      for host in hosts)
        if not queued and not running:
            break
        if not hosts:
            raise RuntimeError(
                f"{len(queued)} batches remain, no live hosts remain, and "
                "the replacement budget is exhausted"
            )

        time.sleep(POLL_SECONDS)
        polls += 1

        active = [
            host for host in hosts
            if host["alive"] and host["ready"] and host["batch"] is not None
        ]
        if active:
            with ThreadPoolExecutor(
                    max_workers=min(PROBE_PARALLEL, len(active))) as pool:
                states = list(pool.map(
                    lambda host: probe(host, port, *host["batch"]), active,
                ))
        else:
            states = []

        progress = []
        checkpoints = []
        for host, state in zip(active, states):
            batch = host["batch"]
            if state["phase"] == "done":
                try:
                    download_batch(host, port, *batch)
                except (subprocess.CalledProcessError, RuntimeError, OSError) as error:
                    host["strikes"] += 1
                    log(f"[{host['name']}] download failed ({error}); retrying")
                    if host["strikes"] >= UNREACHABLE_LIMIT:
                        requeue(host, "download kept failing")
                    continue
                downloaded += 1
                log(
                    f"[{host['name']}] batch {batch_key(*batch)} downloaded "
                    f"({downloaded}/{len(batches)}; {len(queued)} queued)"
                )
                checkpoint_path(*batch).unlink(missing_ok=True)
                host["batch"] = None
                host["strikes"] = 0
            elif state["phase"] == "running":
                host["strikes"] = 0
                item = state.get("progress") or {}
                if item.get("samples_total"):
                    progress.append((
                        host,
                        item.get("samples_done", 0),
                        item["samples_total"],
                        item.get("elapsed_sec", 0),
                    ))
                if polls % CHECKPOINT_PULL_EVERY == 0:
                    checkpoints.append((host, batch))
            elif state["phase"] in ("failed", "missing"):
                host["strikes"] += 1
                if state["phase"] == "failed" \
                        or host["strikes"] >= MISSING_LIMIT:
                    if host["restarts"] < MAX_RESTARTS:
                        host["restarts"] += 1
                        host["strikes"] = 0
                        log(
                            f"[{host['name']}] unit died "
                            f"({state.get('detail', 'no status')}); restart "
                            f"{host['restarts']}/{MAX_RESTARTS}"
                        )
                        try:
                            start_job(host, port, *batch)
                        except (subprocess.CalledProcessError, OSError):
                            pull_checkpoint(host, port, *batch)
                            requeue(host, "restart failed")
                    else:
                        pull_checkpoint(host, port, *batch)
                        requeue(host, "unit kept dying")
            elif state["phase"] == "unreachable":
                host["strikes"] += 1
                if host["strikes"] % 10 == 0:
                    log(
                        f"[{host['name']}] unreachable "
                        f"({host['strikes']}/{UNREACHABLE_LIMIT})"
                    )
                if host["strikes"] >= UNREACHABLE_LIMIT:
                    requeue(host, "unreachable too long")

        save_assignments(hosts)
        if progress:
            samples_done = sum(item[1] for item in progress)
            samples_total = sum(item[2] for item in progress)
            log(
                f"fleet: {len(progress)} running "
                f"{samples_done:,}/{samples_total:,} current-batch samples; "
                f"{downloaded}/{len(batches)} batches downloaded; "
                f"{len(queued)} queued"
            )
        if polls % CHECKPOINT_PULL_EVERY == 0:
            for host, done, total, elapsed in progress:
                log(
                    f"[{host['name']}] batch {batch_key(*host['batch'])}: "
                    f"{done:,}/{total:,} samples ({elapsed:.0f}s)"
                )
        if checkpoints:
            with ThreadPoolExecutor(
                    max_workers=min(PROBE_PARALLEL, len(checkpoints))) as pool:
                list(pool.map(
                    lambda item: pull_checkpoint(item[0], port, *item[1]),
                    checkpoints,
                ))

    missing = [batch for batch in batches if not batch_done_locally(*batch)]
    if missing:
        raise RuntimeError(
            f"{len(missing)} batches are incomplete: "
            + ", ".join(batch_key(*batch) for batch in missing[:5])
        )

    log("All batches downloaded; collating...")
    collate(batches, args.total_samples)
    if args.keep_infra:
        log("--keep-infra set; leaving the live fleet in place")
    elif kept_failed:
        log(
            f"{len(kept_failed)} failed host(s) kept for inspection; "
            f"destroy with: terraform -chdir={INFRA_DIR} destroy"
        )
    else:
        log("Destroying remaining Terraform resources...")
        maybe_destroy()
    log("Done")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--total-samples", type=int, default=DEFAULT_TOTAL_SAMPLES,
        help="total Monte Carlo samples",
    )
    ap.add_argument(
        "--batch-samples", type=int, default=DEFAULT_BATCH_SAMPLES,
        help="samples per resumable batch/job",
    )
    ap.add_argument(
        "--hosts", type=int, default=2,
        help="live worker cap (the fleet never exceeds pending batches)",
    )
    ap.add_argument(
        "--keep-infra", action="store_true",
        help="retain live hosts and shared infrastructure after success",
    )
    ap.add_argument(
        "--keep-failed", action="store_true",
        help="retain failed hosts for inspection (outside the live host cap)",
    )
    args = ap.parse_args()
    if args.total_samples <= 0:
        ap.error("--total-samples must be positive")
    if args.batch_samples <= 0:
        ap.error("--batch-samples must be positive")
    if args.hosts <= 0:
        ap.error("--hosts must be positive")

    warn_dirty()
    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        log(f"FAILED: {error}")
        log(
            "Terraform state and downloaded checkpoints were preserved; "
            "rerun to resume or inspect the remaining hosts"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted. State is saved; rerun to resume.", file=sys.stderr)
        raise SystemExit(130)
