locals {
  # Hetzner network zone for each supported location.
  network_zones = {
    fsn1 = "eu-central"
    nbg1 = "eu-central"
    hel1 = "eu-central"
    ash  = "us-east"
    hil  = "us-west"
    sin  = "ap-southeast"
  }
  # Node 0 is the coordinator; all private IPs are assigned statically.
  private_ip = { for i in range(var.instance_count) : i => "10.0.1.${i + 10}" }
}

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

# Private network so workers can reach the coordinator's queue without
# exposing it publicly (Hetzner firewalls only filter the public interface).
resource "hcloud_network" "internal" {
  name     = var.name
  ip_range = "10.0.0.0/16"
  labels   = { "createdby" : "${var.name}-terraform" }
}

resource "hcloud_network_subnet" "internal" {
  network_id   = hcloud_network.internal.id
  type         = "cloud"
  network_zone = local.network_zones[var.location]
  ip_range     = "10.0.1.0/24"
}

resource "hcloud_server" "node" {
  count        = var.instance_count
  name         = "${var.name}-${count.index}"
  image        = "ubuntu-26.04"
  server_type  = var.instance_type
  location     = var.location
  firewall_ids = [hcloud_firewall.main.id]
  user_data    = data.cloudinit_config.config.rendered
  ssh_keys     = [hcloud_ssh_key.admin.id]
  labels       = { "createdby" : "${var.name}-terraform" }

  network {
    network_id = hcloud_network.internal.id
    ip         = local.private_ip[count.index]
  }

  depends_on = [hcloud_network_subnet.internal]

  # Avoid recreating the server for these, should change these manually (ansible, etc)
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
  description = "Hosts (index 0 is the coordinator)"
  value = [
    for index, server in hcloud_server.node : {
      index      = index
      name       = server.name
      ipv4       = server.ipv4_address
      private_ip = local.private_ip[index]
    }
  ]
}

output "ssh_port" {
  value = var.ssh_port
}
