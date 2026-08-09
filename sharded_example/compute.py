#!/usr/bin/env python3
"""One shard of a Monte Carlo pi estimation.

Draws random points in the unit square and counts how many land inside the
quarter circle; pi ~= 4 * inside / samples. Each shard handles an equal slice
of the total samples, split into chunks that run in parallel across all local
CPUs. Progress and results are written to --out-dir as JSON so an external
orchestrator can watch the job without keeping a connection open:

  status.json   {"state": "running" | "done" | "failed", ...}
  progress.json updated after every finished chunk
  result.json   written once on success

Stdlib only; the default total (800M samples across all shards) takes about a
minute on 4 vCPUs.
"""

import argparse
import json
import multiprocessing
import os
import random
import time
import traceback
from pathlib import Path

CHUNK_SAMPLES = 10_000_000


def atomic_write(path: Path, obj) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    os.replace(tmp, path)


def sample_chunk(args):
    seed, n = args
    rand = random.Random(seed).random
    inside = 0
    for _ in range(n):
        x = rand()
        y = rand()
        if x * x + y * y <= 1.0:
            inside += 1
    return n, inside


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shard-index", type=int, required=True)
    ap.add_argument("--num-shards", type=int, required=True)
    ap.add_argument("--total-samples", type=int, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args()

    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    atomic_write(
        out / "status.json",
        {"state": "running", "shard_index": args.shard_index, "started_at": time.time()},
    )

    try:
        base, remainder = divmod(args.total_samples, args.num_shards)
        shard_samples = base + (1 if args.shard_index < remainder else 0)

        # Deterministic, non-overlapping seeds across shards and chunks.
        chunks = []
        remaining = shard_samples
        while remaining > 0:
            n = min(CHUNK_SAMPLES, remaining)
            chunks.append((args.shard_index * 1_000_000 + len(chunks), n))
            remaining -= n

        start = time.time()
        samples_done = 0
        inside_total = 0
        progress = {
            "chunks_done": 0,
            "chunks_total": len(chunks),
            "samples_done": 0,
            "samples_total": shard_samples,
            "elapsed_sec": 0.0,
        }
        atomic_write(out / "progress.json", progress)

        with multiprocessing.Pool() as pool:
            for n, inside in pool.imap_unordered(sample_chunk, chunks):
                samples_done += n
                inside_total += inside
                progress["chunks_done"] += 1
                progress["samples_done"] = samples_done
                progress["elapsed_sec"] = round(time.time() - start, 1)
                atomic_write(out / "progress.json", progress)

        atomic_write(
            out / "result.json",
            {
                "shard_index": args.shard_index,
                "samples": samples_done,
                "inside": inside_total,
                "pi_partial": 4 * inside_total / samples_done,
                "elapsed_sec": round(time.time() - start, 1),
            },
        )
        atomic_write(
            out / "status.json",
            {"state": "done", "shard_index": args.shard_index, "finished_at": time.time()},
        )
    except Exception:
        atomic_write(
            out / "status.json",
            {"state": "failed", "shard_index": args.shard_index, "error": traceback.format_exc()},
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
