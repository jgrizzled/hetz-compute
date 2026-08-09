locals {
  # Hetzner datacenter mappings
  datacenter_config = {
    "ash" = { # Ashburn, VA
      timezone = "America/New_York"
    }
    "hil" = { # Hillsboro, OR
      timezone = "America/Los_Angeles"
    }
    "fsn1" = { # Falkenstein, Germany
      timezone = "Europe/Berlin"
    }
    "nbg1" = { # Nuremberg, Germany
      timezone = "Europe/Berlin"
    }
    "hel1" = { # Helsinki, Finland
      timezone = "Europe/Helsinki"
    }
    "sin" = { # Singapore
      timezone = "Asia/Singapore"
    }
  }

  dc_config = local.datacenter_config[var.location]
}