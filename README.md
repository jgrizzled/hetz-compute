# Hetzner Compute

Terraform and stdlib-Python examples for running ephemeral multi-server
compute jobs on Hetzner Cloud.

## Layout

```
infra_template/   reusable Terraform/cloud-init starting point
queued_example/   pull-based work queue with a persistent coordinator
sharded_example/  coordinator-free, resumable batches on an elastic fleet
```

The shared infrastructure defaults to key-only SSH behind a source-filtered
Hetzner firewall, a host firewall, bounded journald storage, package updates,
and location-appropriate timezone/network settings.

The examples deploy the current Git `HEAD` over SSH and run work in detached
systemd units. Their orchestrators use bounded SSH sessions and concurrent
host operations so an unreachable or slow machine cannot freeze a large
fleet.

## Usage

Copy `infra_template/` into a project, then copy
`terraform.tfvars.example` to `terraform.tfvars` and fill in its placeholders.
Secrets, Terraform state, provider caches, and generated auto-tfvars files are
gitignored.

See each example's README for its execution and recovery model. Use the
sharded example for long, independently reproducible batches; use the queued
example when many smaller tasks should be leased dynamically by workers.
