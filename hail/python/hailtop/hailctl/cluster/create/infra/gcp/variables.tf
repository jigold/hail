variable cluster_name {
  type = string
}

variable gcp_project {
  type = string
}

variable gcp_region {
  type = string
}

variable gcp_zone {
  type = string
}

variable gcp_location {
  type = string
  default = "us"
}

variable organization_domain {
  type = string
}

variable username {
  type = string
}

variable email {
  type = string
}

variable n_cores {
  type = number
  default = 2
}

variable boot_disk_size {
  type = number
  default = 75
}

variable machine_family {
  type = string
  default = "n1"
}

variable machine_type {
  type = string
  default = "standard"
}

variable db_cores {
  type = number
  default = 1
}

variable db_memory {
  type = number
  default = 3840
}

variable bucket_storage_class {
  type = string
}

variable bucket_location {
  type = string
}

variable oauth2_credentials_file {
  type = string
}

variable repo {
  type = string
}

variable commit {
  type = string
}

variable tf_state_bucket {
  type = string
}
