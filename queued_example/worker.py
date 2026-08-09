#!/usr/bin/env python3
"""Queue worker: pulls Monte Carlo pi tasks from the coordinator's work queue.

Spawns one worker process per CPU minus --reserve-cpus (the coordinator
reserves one core for the queue server). Each process loops: lease a task,
count random points inside the quarter circle, post the count back. A process
exits 0 once the queue reports every task done (HTTP 410), and 1 if the queue
stays unreachable. Holds no local state -- the queue owns all bookkeeping, so
a killed worker just forfeits its lease. Stdlib only.
"""

import argparse
import json
import multiprocessing
import os
import random
import socket
import sys
import time
import urllib.error
import urllib.request

RETRY_SEC = 2
CONN_FAILURE_LIMIT = 30  # consecutive connection failures before giving up


def http(url: str, payload=None):
    """Return (status, body). Raises HTTPError/URLError/OSError."""
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, r.read()


def count_inside(seed: int, samples: int) -> int:
    rand = random.Random(seed).random
    inside = 0
    for _ in range(samples):
        x = rand()
        y = rand()
        if x * x + y * y <= 1.0:
            inside += 1
    return inside


def post_result(queue_url: str, result: dict) -> bool:
    for _ in range(CONN_FAILURE_LIMIT):
        try:
            http(f"{queue_url}/result", payload=result)
            return True
        except (urllib.error.URLError, OSError):
            time.sleep(RETRY_SEC)
    return False


def work_loop(queue_url: str, worker_id: str) -> int:
    misses = 0
    while True:
        try:
            status, body = http(f"{queue_url}/task?worker={worker_id}")
        except urllib.error.HTTPError as e:
            if e.code == 410:  # every task is done
                return 0
            status = None  # transient server error; retry
        except (urllib.error.URLError, OSError):
            status = None
        if status is None:
            misses += 1
            if misses >= CONN_FAILURE_LIMIT:
                print(f"{worker_id}: queue unreachable, giving up", file=sys.stderr)
                return 1
            time.sleep(RETRY_SEC)
            continue
        misses = 0
        if status != 200:  # 204: nothing pending, but leased tasks may still expire
            time.sleep(RETRY_SEC)
            continue

        task = json.loads(body)
        start = time.time()
        inside = count_inside(task["seed"], task["samples"])
        done = post_result(queue_url, {
            "id": task["id"],
            "samples": task["samples"],
            "inside": inside,
            "worker": worker_id,
            "elapsed_sec": round(time.time() - start, 2),
        })
        if not done:
            print(f"{worker_id}: could not post result, giving up", file=sys.stderr)
            return 1


def run_process(queue_url: str, worker_id: str) -> None:
    sys.exit(work_loop(queue_url, worker_id))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--queue-url", required=True, help="e.g. http://10.0.1.10:8080")
    ap.add_argument("--reserve-cpus", type=int, default=0,
                    help="CPUs to leave free (coordinator reserves one for the queue)")
    args = ap.parse_args()

    procs = max(1, (os.cpu_count() or 1) - args.reserve_cpus)
    hostname = socket.gethostname()
    print(f"{hostname}: starting {procs} worker process(es)", flush=True)
    children = [
        multiprocessing.Process(target=run_process, args=(args.queue_url, f"{hostname}:{i}"))
        for i in range(procs)
    ]
    for c in children:
        c.start()
    for c in children:
        c.join()
    sys.exit(max((c.exitcode or 0) for c in children))


if __name__ == "__main__":
    main()
