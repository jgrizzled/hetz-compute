#!/usr/bin/env python3
"""Compute one resumable batch of a Monte Carlo pi estimation.

The batch is identified by its half-open global sample range. Work is split
into CPU-sized chunks and checkpointed after every completed chunk, so the
same systemd job can be restarted or moved to another host without starting
the batch over. All files are batch-specific, allowing a host to process
several batches in sequence:

  status-START-END.json      running | done | failed
  progress-START-END.json    completed chunks and samples
  checkpoint-START-END.json  completed chunk results (restart input)
  result-START-END.json      written once the complete batch is verified

Stdlib only.
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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    os.replace(tmp, path)


def sample_chunk(args):
    offset, samples = args
    rand = random.Random(offset).random
    inside = 0
    for _ in range(samples):
        x = rand()
        y = rand()
        if x * x + y * y <= 1.0:
            inside += 1
    return offset, samples, inside


def make_chunks(start: int, end: int, chunk_samples: int):
    chunks = []
    offset = start
    while offset < end:
        samples = min(chunk_samples, end - offset)
        chunks.append((offset, samples))
        offset += samples
    return chunks


def load_checkpoint(path: Path, start: int, end: int, chunk_samples: int,
                    expected_chunks) -> dict:
    if not path.exists():
        return {}
    checkpoint = json.loads(path.read_text())
    expected_meta = {
        "sample_start": start,
        "sample_end": end,
        "chunk_samples": chunk_samples,
    }
    for key, value in expected_meta.items():
        if checkpoint.get(key) != value:
            raise ValueError(
                f"checkpoint {path} has {key}={checkpoint.get(key)!r}; "
                f"expected {value!r}"
            )

    expected = {str(offset): samples for offset, samples in expected_chunks}
    completed = checkpoint.get("chunks")
    if not isinstance(completed, dict):
        raise ValueError(f"checkpoint {path} has no chunk map")
    for offset, result in completed.items():
        if offset not in expected or not isinstance(result, dict):
            raise ValueError(f"checkpoint {path} contains unknown chunk {offset}")
        samples = result.get("samples")
        inside = result.get("inside")
        if samples != expected[offset] or not isinstance(inside, int) \
                or not 0 <= inside <= samples:
            raise ValueError(f"checkpoint {path} contains invalid chunk {offset}")
    return completed


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample-start", type=int, required=True)
    ap.add_argument("--sample-end", type=int, required=True)
    ap.add_argument("--chunk-samples", type=int, default=CHUNK_SAMPLES)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args()

    if args.sample_start < 0 or args.sample_end <= args.sample_start:
        ap.error("the sample range must satisfy 0 <= START < END")
    if args.chunk_samples <= 0:
        ap.error("--chunk-samples must be positive")

    key = f"{args.sample_start}-{args.sample_end}"
    out = args.out_dir
    status_path = out / f"status-{key}.json"
    progress_path = out / f"progress-{key}.json"
    checkpoint_path = out / f"checkpoint-{key}.json"
    result_path = out / f"result-{key}.json"
    out.mkdir(parents=True, exist_ok=True)

    started_at = time.time()
    atomic_write(status_path, {
        "state": "running",
        "sample_start": args.sample_start,
        "sample_end": args.sample_end,
        "started_at": started_at,
    })

    try:
        chunks = make_chunks(args.sample_start, args.sample_end,
                             args.chunk_samples)
        completed = load_checkpoint(
            checkpoint_path, args.sample_start, args.sample_end,
            args.chunk_samples, chunks,
        )
        resumed_chunks = len(completed)

        def write_progress() -> None:
            samples_done = sum(item["samples"] for item in completed.values())
            atomic_write(progress_path, {
                "chunks_done": len(completed),
                "chunks_total": len(chunks),
                "samples_done": samples_done,
                "samples_total": args.sample_end - args.sample_start,
                "resumed_chunks": resumed_chunks,
                "elapsed_sec": round(time.time() - started_at, 1),
            })

        write_progress()
        pending = [(offset, samples) for offset, samples in chunks
                   if str(offset) not in completed]
        if pending:
            with multiprocessing.Pool() as pool:
                for offset, samples, inside in pool.imap_unordered(
                        sample_chunk, pending):
                    completed[str(offset)] = {
                        "samples": samples,
                        "inside": inside,
                    }
                    atomic_write(checkpoint_path, {
                        "sample_start": args.sample_start,
                        "sample_end": args.sample_end,
                        "chunk_samples": args.chunk_samples,
                        "chunks": completed,
                    })
                    write_progress()

        samples_done = sum(item["samples"] for item in completed.values())
        inside_total = sum(item["inside"] for item in completed.values())
        expected_samples = args.sample_end - args.sample_start
        if len(completed) != len(chunks) or samples_done != expected_samples:
            raise RuntimeError(
                f"incomplete batch: {len(completed)}/{len(chunks)} chunks, "
                f"{samples_done}/{expected_samples} samples"
            )

        atomic_write(result_path, {
            "sample_start": args.sample_start,
            "sample_end": args.sample_end,
            "samples": samples_done,
            "inside": inside_total,
            "pi_partial": 4 * inside_total / samples_done,
            "chunks": len(chunks),
            "resumed_chunks": resumed_chunks,
            "elapsed_sec": round(time.time() - started_at, 1),
        })
        atomic_write(status_path, {
            "state": "done",
            "sample_start": args.sample_start,
            "sample_end": args.sample_end,
            "finished_at": time.time(),
        })
    except Exception:
        atomic_write(status_path, {
            "state": "failed",
            "sample_start": args.sample_start,
            "sample_end": args.sample_end,
            "error": traceback.format_exc(),
        })
        raise SystemExit(1)


if __name__ == "__main__":
    main()
