#cloud-config

users:
  - name: admin
    primary_group: admin
    groups: [adm, sudo]
    shell: /bin/bash
    lock_passwd: true
    sudo: ALL=(ALL) NOPASSWD:ALL
    ssh_authorized_keys:
      - ${ssh_public_key}

package_update: true
package_upgrade: true
packages:
  - ufw
  - git

write_files:
  - path: /etc/ssh/sshd_config.d/cloudinit.conf
    content: |
      Port ${ssh_port}
      PasswordAuthentication no
      PermitRootLogin no
      X11Forwarding no
      MaxAuthTries 10

timezone: ${tz}

runcmd:
  - [systemctl, daemon-reload]
  - [systemctl, restart, ssh]

  - [ufw, default, deny, incoming]
  - [ufw, default, allow, outgoing]
  # Do not use `ufw limit` here. Fleet setup creates several connections in
  # quick succession, while source filtering is already enforced by the
  # Hetzner cloud firewall's ssh_allowed_ips rule.
  - [ufw, allow, "${ssh_port}/tcp", comment, "SSH"]
  - [ufw, --force, enable]

  # Keep long-running compute logs from filling the root disk.
  - [
      sed,
      "-i",
      "s/#SystemMaxUse=/SystemMaxUse=3G/g",
      /etc/systemd/journald.conf,
    ]
  - [
      sed,
      "-i",
      "s/#MaxRetentionSec=/MaxRetentionSec=1week/g",
      /etc/systemd/journald.conf,
    ]
