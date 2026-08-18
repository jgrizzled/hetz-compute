locals {
  datacenter_config = {
    ash  = { timezone = "America/New_York" }
    hil  = { timezone = "America/Los_Angeles" }
    fsn1 = { timezone = "Europe/Berlin" }
    nbg1 = { timezone = "Europe/Berlin" }
    hel1 = { timezone = "Europe/Helsinki" }
    sin  = { timezone = "Asia/Singapore" }
  }

  dc_config = local.datacenter_config[var.location]
}
