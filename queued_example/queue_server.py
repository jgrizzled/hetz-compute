#!/usr/bin/env python3
"""Work queue server; runs on the coordinator host.

Tasks are Monte Carlo pi sampling chunks. Workers lease one task at a time
over HTTP and post results back:

  GET  /task?worker=ID  200 {"id","seed","samples"} | 204 retry later | 410 all done
  POST /result          body {"id","samples","inside","worker","elapsed_sec"}
  GET  /status          queue counters, for the orchestrator's polling
  GET  /state           the full queue state

The entire queue lives in one JSON file, rewritten atomically on every change,
so a restarted server resumes exactly where it left off (--num-tasks and
--samples-per-task are ignored when a state file already exists). Leases
expire after LEASE_TIMEOUT seconds, so tasks held by a crashed worker are
handed out again. Stdlib only.
"""

import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

LEASE_TIMEOUT = 60


class WorkQueue:
    def __init__(self, path: Path, num_tasks: int, samples_per_task: int):
        self.path = path
        self.lock = threading.Lock()
        if path.exists():
            self.state = json.loads(path.read_text())
        else:
            self.state = {
                "num_tasks": num_tasks,
                "samples_per_task": samples_per_task,
                "pending": list(range(num_tasks)),
                "leases": {},  # task id (str) -> {"worker", "ts"}
                "done": {},  # task id (str) -> result
            }
            path.parent.mkdir(parents=True, exist_ok=True)
            self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        os.replace(tmp, self.path)

    def reclaim_expired(self) -> None:
        now = time.time()
        expired = [t for t, l in self.state["leases"].items() if now - l["ts"] > LEASE_TIMEOUT]
        for tid in expired:
            del self.state["leases"][tid]
            self.state["pending"].append(int(tid))

    def lease(self, worker: str):
        """Return a task dict, "done" when everything is finished, or None (retry)."""
        with self.lock:
            self.reclaim_expired()
            if self.state["pending"]:
                tid = self.state["pending"].pop(0)
                self.state["leases"][str(tid)] = {"worker": worker, "ts": time.time()}
                self.save()
                return {"id": tid, "seed": tid, "samples": self.state["samples_per_task"]}
            if len(self.state["done"]) >= self.state["num_tasks"]:
                return "done"
            return None

    def submit(self, result: dict) -> None:
        with self.lock:
            tid = str(result["id"])
            self.state["leases"].pop(tid, None)
            if int(tid) in self.state["pending"]:  # expired lease got requeued
                self.state["pending"].remove(int(tid))
            if tid not in self.state["done"]:  # first submission wins
                self.state["done"][tid] = result
            self.save()

    def status(self) -> dict:
        with self.lock:
            self.reclaim_expired()
            return {
                "num_tasks": self.state["num_tasks"],
                "pending": len(self.state["pending"]),
                "leased": len(self.state["leases"]),
                "done": len(self.state["done"]),
            }


class Handler(BaseHTTPRequestHandler):
    queue: WorkQueue = None  # set in main()

    def reply(self, code: int, obj=None) -> None:
        body = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/task":
            worker = parse_qs(url.query).get("worker", ["unknown"])[0]
            task = self.queue.lease(worker)
            if task == "done":
                self.reply(410)
            elif task is None:
                self.reply(204)
            else:
                self.reply(200, task)
        elif url.path == "/status":
            self.reply(200, self.queue.status())
        elif url.path == "/state":
            with self.queue.lock:
                self.reply(200, self.queue.state)
        else:
            self.reply(404)

    def do_POST(self) -> None:
        if urlparse(self.path).path != "/result":
            self.reply(404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            result = json.loads(body)
            if not isinstance(result.get("id"), int):
                raise ValueError("missing task id")
        except ValueError:
            self.reply(400)
            return
        self.queue.submit(result)
        self.reply(200, {"ok": True})

    def log_message(self, *args) -> None:  # keep the journal quiet
        pass


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--state-file", type=Path, required=True)
    ap.add_argument("--num-tasks", type=int, required=True)
    ap.add_argument("--samples-per-task", type=int, required=True)
    args = ap.parse_args()

    Handler.queue = WorkQueue(args.state_file, args.num_tasks, args.samples_per_task)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"queue serving on :{args.port}, state in {args.state_file}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
