variable bucket_location {
  type = string
}

variable bucket_storage_class {
  type = string
}

variable tf_state_bucket {
  type = string
}

variable db_cores {
  type = number
  default = 1
}

variable db_memory {
  type = number
  default = 3840
}
