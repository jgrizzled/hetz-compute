# Sharded compute example

This example runs deterministic, resumable Monte Carlo batches on an elastic
Hetzner Cloud fleet. `run.py` provisions up to `--hosts` servers, deploys the
current Git commit, assigns one detached systemd batch at a time to each ready
host, verifies every result, collates the estimate, and tears the fleet down.

Unlike the queued example, there is no remote coordinator or service: the
local orchestrator owns a small batch queue. This works well when each batch
is long-lived and independently reproducible.

## Layout

```
run.py      local orchestrator (stdlib only)
compute.py  resumable batch worker (runs on each host)
infra/      Terraform; workers are keyed by stable ids
state/      assignments, staged checkpoints, results, and collation (gitignored)
```

## Work and failure model

The half-open sample range `0..--total-samples` is cut into
`--batch-samples` jobs. Faster hosts take more jobs. The fleet starts no
larger than the number of pending batches, and, by default, a host is removed
as soon as the queue is empty and that host becomes idle.

Each worker checkpoints after every CPU chunk. A reachable host whose unit
dies is restarted in place; an unreachable or repeatedly failing host has its
batch requeued and is replaced. Checkpoints are periodically staged under
`state/checkpoints/` and uploaded to a replacement. A batch that fails on
three hosts aborts as a likely systematic error. Downloads are validated and
written atomically before they count as complete.

Host setup and polling are bounded and concurrent. Each host becomes eligible
for work as soon as its own cloud-init and deployment finish, rather than
waiting behind the slowest host in the fleet.

## Prerequisites

- `terraform`, `git`, and Python 3 on `PATH`.
- A Hetzner Cloud API token and an SSH key pair.
- `infra/terraform.tfvars`, copied from `infra/terraform.tfvars.example`, with
  the token, public key, and your public IP filled in. The matching private key
  must be available through ssh-agent or normal SSH configuration.
- A committed `HEAD`. Hosts receive the latest commit, not working-tree
  changes.

Fleet size is not stored in `terraform.tfvars`; `run.py --hosts` maintains the
gitignored `infra/hosts.auto.tfvars.json` file.

## Run

```sh
python3 run.py
```

The defaults run 800 million samples as eight 100-million-sample batches on
at most two cpx22 hosts. Tune the workload and fleet independently:

```sh
python3 run.py --total-samples 1600000000 --batch-samples 100000000 --hosts 8
```

Keep at least as many batches as hosts if you want every host to contribute.
For a cheap end-to-end orchestration smoke test:

```sh
python3 run.py --total-samples 100000 --batch-samples 50000 --hosts 1
```

Use `--keep-infra` to retain live hosts after success. Use `--keep-failed` to
retain failed hosts for inspection while replacement hosts continue the run;
retained failed hosts are intentionally outside the live `--hosts` cap.

## Resuming

The workflow has no required in-memory-only state:

- Terraform and `infra/hosts.auto.tfvars.json` preserve stable host ids.
- `state/assignments.json` reconnects hosts to in-flight batches.
- Remote and locally staged checkpoints preserve completed chunks.
- Valid files under `state/results/` mark batches complete.

Interrupt and rerun the same command to resume. To start a different run,
destroy any retained infrastructure and move or remove `state/` so results
from matching sample ranges are not reused.
