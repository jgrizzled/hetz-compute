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
  - curl
  - git
  - gcc
  - tar
  - tmux
  - htop

write_files:
  - path: /etc/ssh/sshd_config.d/cloudinit.conf
    content: |
      Port ${ssh_port}
      PasswordAuthentication no
      PermitRootLogin no
      X11Forwarding no
      MaxAuthTries 10
      AllowTcpForwarding yes
      AllowAgentForwarding yes

timezone: ${tz}

runcmd:
  - [systemctl, daemon-reload]
  - [systemctl, restart, ssh]

  - [ufw, default, deny, incoming]
  - [ufw, default, allow, outgoing]
  # Allow local incoming connections
  - [ufw, allow, from, 10.0.0.0/24]
  # Rate-limit SSH
  - [ufw, limit, "${ssh_port}/tcp", comment, "Rate-limit SSH"]
  - [ufw, --force, enable]

  # Bounds the amount of logs that can survive on the system
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

  # Install uv for the admin user
  - [sudo, -u, admin, sh, -c, "curl -LsSf https://astral.sh/uv/install.sh | sh"]

  # github.com host key so the first ssh clone is non-interactive
  - [
      sudo,
      -u,
      admin,
      sh,
      -c,
      "mkdir -p ~/.ssh && chmod 700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null",
    ]
