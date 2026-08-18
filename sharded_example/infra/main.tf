resource "hcloud_ssh_key" "admin" {
  name       = "${var.name}-admin"
  public_key = var.ssh_public_key
  labels     = { "createdby" : "${var.name}-terraform" }
  lifecycle {
    ignore_changes = [
      public_key
    ]
  }
}

resource "hcloud_firewall" "main" {
  name = var.name
  rule {
    description = "Allow Incoming ICMP Ping Requests"
    direction   = "in"
    protocol    = "icmp"
    port        = ""
    source_ips  = ["0.0.0.0/0", "::/0"]
  }
  rule {
    description     = "Allow Outbound ICMP Ping Requests"
    direction       = "out"
    protocol        = "icmp"
    port            = ""
    destination_ips = ["0.0.0.0/0", "::/0"]
  }
  rule {
    description = "Allow Incoming SSH Traffic"
    direction   = "in"
    protocol    = "tcp"
    port        = var.ssh_port
    source_ips  = var.ssh_allowed_ips
  }
  rule {
    description     = "Allow All Outbound TCP Traffic"
    direction       = "out"
    protocol        = "tcp"
    port            = "any"
    destination_ips = ["0.0.0.0/0", "::/0"]
  }
  rule {
    description     = "Allow All Outbound UDP Traffic"
    direction       = "out"
    protocol        = "udp"
    port            = "any"
    destination_ips = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "worker" {
  # Stable ids let run.py remove one idle/dead host without renumbering or
  # replacing the rest of the fleet.
  for_each     = toset(var.host_ids)
  name         = "${var.name}-${each.key}"
  image        = "ubuntu-26.04"
  server_type  = var.instance_type
  location     = var.location
  firewall_ids = [hcloud_firewall.main.id]
  user_data    = data.cloudinit_config.config.rendered
  ssh_keys     = [hcloud_ssh_key.admin.id]
  labels       = { "createdby" : "${var.name}-terraform" }

  # Avoid recreating the server for these; change them manually if needed.
  lifecycle {
    ignore_changes = [
      user_data,
      image,
      ssh_keys
    ]
  }
}

data "cloudinit_config" "config" {
  gzip          = true
  base64_encode = true

  part {
    filename     = "init.cfg"
    content_type = "text/cloud-config"
    content = templatefile(
      "${path.module}/cloudinit.yaml.tpl",
      {
        ssh_port       = var.ssh_port
        ssh_public_key = var.ssh_public_key
        tz             = local.dc_config.timezone
      }
    )
  }
}

terraform {
  backend "local" {}
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = ">= 1.68.0"
    }
    cloudinit = {
      source  = "hashicorp/cloudinit"
      version = ">= 2.4.0"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

output "hosts" {
  description = "Hosts"
  value = [
    for id, server in hcloud_server.worker : {
      index = tonumber(id)
      name  = server.name
      ipv4  = server.ipv4_address
    }
  ]
}

output "ssh_port" {
  value = var.ssh_port
}
