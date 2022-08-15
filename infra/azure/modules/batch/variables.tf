variable resource_group {
  type = object({
    id       = string
    name     = string
    location = string
  })
}

variable container_registry_id {
  type = string
}

variable key_vault_id {
  type = string
}

variable key_vault_name {
  type = string
}
