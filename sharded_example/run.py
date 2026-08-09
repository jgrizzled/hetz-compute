#!/usr/bin/env python3
"""Run a sharded Monte Carlo pi computation on Hetzner Cloud.

Flow:
  1. terraform apply (idempotent -- existing servers are reused)
  2. wait for cloud-init to finish on every host
  3. push the current git HEAD to every host over ssh
  4. start one shard per host as a detached transient systemd unit
  5. poll progress, reporting crashes and failures
  6. download and collate results, then terraform destroy

Resumable: terraform state tracks the servers, remote status files track the
jobs, and downloaded results under state/ mark finished shards. Ctrl-C and
rerun at any point to pick up where it left off. Delete state/ to start a
fresh computation.
"""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
INFRA_DIR = HERE / "infra"
STATE_DIR = HERE / "state"
RESULTS_DIR = STATE_DIR / "results"

SSH_USER = "admin"
REMOTE_BARE = "/home/admin/app.git"
REMOTE_TREE = "/home/admin/app"
REMOTE_OUT = "/home/admin/shard_out"
UNIT = "shard-job"

# ~1 minute across 4 vCPUs; tune for your instance type/count.
DEFAULT_TOTAL_SAMPLES = 800_000_000

POLL_SECONDS = 8
CLOUD_INIT_TIMEOUT = 1200
UNREACHABLE_LIMIT = 30  # consecutive failed polls before a host counts as failed
MISSING_LIMIT = 3  # consecutive "no job, no result" polls before failing

# Hosts are ephemeral and their IPs get recycled, so skip host key checking.
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
    "-o", "ConnectTimeout=10",
]

PROBE_CMD = (
    f"systemctl show {UNIT}.service --property=ActiveState,Result 2>/dev/null; "
    f"echo @@@; cat {REMOTE_OUT}/status.json 2>/dev/null; "
    f"echo; echo @@@; cat {REMOTE_OUT}/progress.json 2>/dev/null; echo"
)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def result_path(index: int) -> Path:
    return RESULTS_DIR / f"shard-{index}.json"


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


def tfvars_instance_count():
    tfvars = INFRA_DIR / "terraform.tfvars"
    if tfvars.exists():
        m = re.search(r"^\s*instance_count\s*=\s*(\d+)", tfvars.read_text(), re.M)
        if m:
            return int(m.group(1))
    return None


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


def start_job(host, port, num_shards, total_samples) -> None:
    inner = (
        f"/usr/bin/python3 {REMOTE_TREE}/sharded_example/compute.py"
        f" --shard-index {host['index']} --num-shards {num_shards}"
        f" --total-samples {total_samples} --out-dir {REMOTE_OUT}"
    )
    ssh_run(
        host, port,
        f"sudo systemctl reset-failed {UNIT}.service 2>/dev/null; "
        f"mkdir -p {REMOTE_OUT} && "
        f"rm -f {REMOTE_OUT}/status.json {REMOTE_OUT}/progress.json {REMOTE_OUT}/result.json && "
        f"sudo systemd-run --quiet --unit={UNIT} --uid={SSH_USER} --gid={SSH_USER} "
        f"--working-directory={REMOTE_TREE}/sharded_example {inner}",
    )


# ----------------------------------------------------------------- monitor

def parse_json(text: str):
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def probe(host, port) -> dict:
    """Combine the systemd unit state with the worker's own status files."""
    r = ssh_run(host, port, PROBE_CMD, check=False)
    if r.returncode != 0:
        return {"phase": "unreachable", "detail": r.stderr.strip()[:200]}
    parts = (r.stdout.split("@@@") + ["", ""])[:3]
    unit = dict(l.split("=", 1) for l in parts[0].strip().splitlines() if "=" in l)
    status = parse_json(parts[1]) or {}
    progress = parse_json(parts[2])

    state = status.get("state")
    active = unit.get("ActiveState", "")
    if state == "done":
        return {"phase": "done"}
    if state == "failed":
        error = status.get("error") or ""
        detail = error.strip().splitlines()[-1] if error.strip() else "worker reported failure"
        return {"phase": "failed", "detail": detail}
    if active in ("active", "activating"):
        return {"phase": "running", "progress": progress}
    if active == "failed":
        return {"phase": "failed", "detail": f"systemd unit failed (Result={unit.get('Result')})"}
    if state == "running":
        # Transient units vanish once they stop; started but no result = crash.
        return {"phase": "failed", "detail": "worker process died without writing a result"}
    return {"phase": "missing"}


def download_result(host, port) -> None:
    r = ssh_run(host, port, f"cat {REMOTE_OUT}/result.json")
    parsed = parse_json(r.stdout)
    if parsed is None:
        raise RuntimeError(f"invalid result.json from {host['name']}")
    result_path(host["index"]).write_text(json.dumps(parsed, indent=2))


def poll_until_done(hosts, port) -> dict:
    terminal = {}
    strikes = {h["index"]: 0 for h in hosts}
    while True:
        for host in hosts:
            i = host["index"]
            if i in terminal:
                continue
            if result_path(i).exists():
                terminal[i] = "done"
                continue
            st = probe(host, port)
            phase = st["phase"]
            if phase == "done":
                download_result(host, port)
                log(f"[{host['name']}] shard {i} done, result downloaded")
                terminal[i] = "done"
            elif phase == "failed":
                log(f"[{host['name']}] shard {i} FAILED: {st['detail']}")
                terminal[i] = "failed"
            elif phase == "running":
                strikes[i] = 0
                p = st.get("progress")
                if p and p.get("samples_total"):
                    pct = 100 * p["samples_done"] / p["samples_total"]
                    log(
                        f"[{host['name']}] running: {p['chunks_done']}/{p['chunks_total']}"
                        f" chunks ({pct:.0f}%, {p['elapsed_sec']}s)"
                    )
                else:
                    log(f"[{host['name']}] running (starting up)")
            elif phase == "unreachable":
                strikes[i] += 1
                log(f"[{host['name']}] unreachable ({strikes[i]}/{UNREACHABLE_LIMIT}): {st['detail']}")
                if strikes[i] >= UNREACHABLE_LIMIT:
                    terminal[i] = "failed"
            else:  # missing
                strikes[i] += 1
                if strikes[i] >= MISSING_LIMIT:
                    log(f"[{host['name']}] shard {i} FAILED: no job and no result on host")
                    terminal[i] = "failed"
        if len(terminal) == len(hosts):
            return terminal
        time.sleep(POLL_SECONDS)


# ------------------------------------------------------------ orchestration

def prepare(host, port, num_shards, total_samples) -> None:
    if result_path(host["index"]).exists():
        log(f"[{host['name']}] result already downloaded, skipping")
        return
    log(f"[{host['name']}] waiting for cloud-init...")
    wait_cloud_init(host, port)
    log(f"[{host['name']}] cloud-init done, pushing code")
    push_code(host, port)
    st = probe(host, port)
    if st["phase"] == "done":
        log(f"[{host['name']}] shard already finished on host")
    elif st["phase"] == "running":
        log(f"[{host['name']}] shard already running, resuming watch")
    else:
        start_job(host, port, num_shards, total_samples)
        log(f"[{host['name']}] started shard {host['index']} of {num_shards}")


def collate(num_shards: int) -> None:
    shards = [json.loads(result_path(i).read_text()) for i in range(num_shards)]
    samples = sum(s["samples"] for s in shards)
    inside = sum(s["inside"] for s in shards)
    pi = 4 * inside / samples
    out = {
        "num_shards": num_shards,
        "total_samples": samples,
        "inside": inside,
        "pi_estimate": pi,
        "abs_error": abs(pi - math.pi),
        "shards": shards,
    }
    (STATE_DIR / "collated.json").write_text(json.dumps(out, indent=2))
    log(f"pi ~= {pi:.8f} (abs error {abs(pi - math.pi):.2e}) "
        f"from {samples:,} samples across {num_shards} shards")
    log(f"collated result written to {STATE_DIR / 'collated.json'}")


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
        help="total Monte Carlo samples across all shards",
    )
    ap.add_argument(
        "--keep-infra", action="store_true",
        help="skip terraform destroy at the end",
    )
    args = ap.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Fast path: previous run already downloaded every result (e.g. it was
    # interrupted between collation and destroy).
    expected = tfvars_instance_count()
    if expected and all(result_path(i).exists() for i in range(expected)):
        log("All shard results already downloaded")
        collate(expected)
        if not args.keep_infra:
            maybe_destroy()
        return

    warn_dirty()
    log("Applying terraform...")
    hosts, port = tf_apply()
    log(f"{len(hosts)} host(s): " + ", ".join(f"{h['name']}={h['ipv4']}" for h in hosts))

    with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
        list(pool.map(lambda h: prepare(h, port, len(hosts), args.total_samples), hosts))

    terminal = poll_until_done(hosts, port)
    failed = sorted(i for i, phase in terminal.items() if phase != "done")
    if failed:
        log(f"Shards failed: {failed}. Leaving servers up for inspection; "
            f"rerun to retry, or clean up with: terraform -chdir={INFRA_DIR} destroy")
        sys.exit(1)

    collate(len(hosts))
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
