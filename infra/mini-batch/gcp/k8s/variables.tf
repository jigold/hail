variable organization_domain {
  type = string
}

variable internal_ip {
  type = string
}

variable external_ip {
  type = string
}

variable batch_gcp_regions {
  type = list(string)
}

variable minikube_host_ip {
  type = string
}
