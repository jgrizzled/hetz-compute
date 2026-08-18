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
  - curl

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
  # The queue is private and only reachable by nodes on this subnet.
  - [ufw, allow, from, 10.0.1.0/24]
  # Do not use `ufw limit`: fleet setup creates several connections in quick
  # succession, and public source filtering already happens in Hetzner's
  # ssh_allowed_ips cloud-firewall rule.
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
