locals {
  # Hetzner datacenter mappings
  datacenter_config = {
    "ash" = { # Ashburn, VA
      timezone     = "America/New_York"
      network_zone = "us-east"
    }
    "hil" = { # Hillsboro, OR
      timezone     = "America/Los_Angeles"
      network_zone = "us-west"
    }
    "fsn1" = { # Falkenstein, Germany
      timezone     = "Europe/Berlin"
      network_zone = "eu-central"
    }
    "nbg1" = { # Nuremberg, Germany
      timezone     = "Europe/Berlin"
      network_zone = "eu-central"
    }
    "hel1" = { # Helsinki, Finland
      timezone     = "Europe/Helsinki"
      network_zone = "eu-central"
    }
    "sin" = { # Singapore
      timezone     = "Asia/Singapore"
      network_zone = "ap-southeast"
    }
  }

  dc_config = local.datacenter_config[var.location]
}
