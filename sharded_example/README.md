# Sharded compute example

Minimal demonstration of a sharded computation on Hetzner Cloud: `run.py`
provisions servers with terraform, deploys the current git commit over ssh,
runs one shard of a Monte Carlo pi estimation per host as a detached systemd
job, watches progress, collates the results, and tears everything down.

## Layout

```
run.py          orchestrator (stdlib only, runs locally)
compute.py      the worker: one shard of the pi estimation (runs on each host)
infra/          terraform (based on ../infra-template)
state/          local run state: downloaded shard results, collated output (gitignored)
```

## Prerequisites

- `terraform` and `git` on your PATH, python3.
- A Hetzner Cloud API token.
- An ssh key pair; the private key must be loadable by plain `ssh` (default
  key or ssh-agent), the public key goes in the tfvars.
- `infra/terraform.tfvars` — copy `infra/terraform.tfvars.example` and fill in
  your token, public key, and allowed IP.

## Run

```sh
python3 run.py
```

What happens, in order:

1. `terraform apply` creates 2x cpx22 (2 vCPU) servers, a firewall, and a
   private network. Idempotent: existing servers are reused.
2. Waits for cloud-init to finish on every host (`cloud-init status --wait`);
   the first boot takes a few minutes because of `package_upgrade`.
3. Pushes the current git HEAD to a bare repo on each host
   (`git push ssh://...`) and checks it out to `~/app`. Uncommitted local
   changes are not deployed — commit first.
4. Starts `compute.py` on each host via `sudo systemd-run --unit=shard-job`,
   so the job survives with no ssh connection open.
5. Polls every few seconds: the systemd unit state plus the worker's own
   `status.json`/`progress.json`. Crashes (worker exception, OOM-kill, unit
   failure) are reported per shard.
6. When a shard finishes, its `result.json` is downloaded to
   `state/results/shard-N.json`. When all shards are done the counts are
   summed into `state/collated.json` and the pi estimate is printed.
7. `terraform destroy` removes all resources. If any shard failed, the
   servers are left up for inspection instead (rerun to retry the failed
   shards, or destroy manually).

The default workload is 800M samples total (~1 minute of compute across the
4 vCPUs); tune with `--total-samples`. Use `--keep-infra` to skip the destroy.

## Resuming

The script keeps no in-memory-only state, so it can be killed and rerun at
any point:

- Servers live in terraform state — `apply` is a no-op if they exist.
- A running/finished job is detected on the host itself (systemd unit +
  status files), so it is never started twice.
- Downloaded results under `state/results/` mark shards as complete; if all
  are present the script skips straight to collation and destroy.

To start a completely fresh run: `rm -rf state/`.
