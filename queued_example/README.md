# Queued compute example

Minimal demonstration of distributed queued work on Hetzner Cloud: `run.py`
provisions servers with terraform, deploys the current git commit over ssh,
starts a work queue on the first server (the coordinator) and a worker on
every server, watches the queue, collates the results, and tears everything
down. Unlike the sharded example, work is not pre-assigned: workers pull
tasks from the queue as fast as they can finish them, so load balances
itself and a crashed worker's tasks are automatically redistributed.

## Layout

```
run.py           orchestrator (stdlib only, runs locally)
queue_server.py  work queue HTTP server (runs on the coordinator, host 0)
worker.py        worker: leases tasks, computes, posts results (runs on every host)
infra/           terraform (based on ../infra-template, plus a private network)
state/           local run state: downloaded queue state, collated output (gitignored)
```

The computation is a Monte Carlo pi estimation split into 60 tasks of 10M
samples each (~600M total, about a minute on the demo's 3 worker vCPUs).

## Prerequisites

- `terraform` and `git` on your PATH, python3.
- A Hetzner Cloud API token.
- An ssh key pair; the public key goes in the tfvars, and the private key
  must be available to ssh normally (ssh-agent or `~/.ssh` config).
- `infra/terraform.tfvars` — copy `infra/terraform.tfvars.example` and fill in
  your token, public key, and allowed IP.

## Run

```sh
python3 run.py
```

What happens, in order:

1. `terraform apply` creates 2x cpx22 (2 vCPU) servers, a firewall, and a
   private network (10.0.1.0/24) so the workers can reach the queue without
   exposing it publicly. Idempotent: existing resources are reused.
2. Waits for cloud-init to finish on every host (`cloud-init status --wait`).
3. Pushes the current git HEAD to a bare repo on each host
   (`git push ssh://...`) and checks it out to `~/app`. Uncommitted local
   changes are not deployed — commit first.
4. Starts `queue_server.py` on the coordinator via
   `sudo systemd-run --unit=work-queue`, then `worker.py` on every host via
   `--unit=queue-worker`, so everything survives with no ssh connection open.
   The coordinator's worker reserves one CPU for the queue server; the other
   host uses all of its CPUs (3 worker processes total).
5. Workers loop: `GET /task` to lease a task, compute it, `POST /result`.
   Leases expire after 60s, so tasks held by a crashed worker are handed
   out again.
6. Polls every few seconds: queue counters (`GET /status`) plus the systemd
   unit state on every host. A crashed worker is reported and the run
   continues on the remaining workers; a dead queue server or dead last
   worker aborts the run, leaving the servers up for inspection.
7. When every task is done the queue state is downloaded to
   `state/queue_final.json`, the counts are summed into `state/results.json`
   (including how many tasks each worker process completed), and the pi
   estimate is printed.
8. `terraform destroy` removes all resources. Use `--keep-infra` to skip.

Tune the workload with `--total-samples` and `--task-samples`.

## Resuming

The script keeps no in-memory-only state, so it can be killed and rerun at
any point:

- Servers live in terraform state — `apply` is a no-op if they exist.
- The queue server persists every lease/result to
  `/home/admin/queue_state/queue.json` (atomic rewrite on each change); if
  the server or the whole coordinator restarts, it resumes from that file.
- Already-running units are detected (`systemctl is-active`) and never
  started twice; a previously crashed unit is `reset-failed` and restarted.
- A downloaded `state/queue_final.json` marks the computation finished; if
  present the script skips straight to collation and destroy.

To start a completely fresh run: `rm -rf state/`, and destroy the servers
(or delete the remote queue state file) so the old queue isn't resumed.

## Watching it fail over

While a run is in progress, kill the non-coordinator worker to watch the
queue reassign its leased tasks to the coordinator's worker:

```sh
ssh -p <ssh_port> admin@<worker-ip> sudo systemctl stop queue-worker
```

`run.py` reports the dead worker and the run finishes (more slowly) on the
remaining CPUs.
