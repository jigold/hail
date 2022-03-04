terraform {
  required_providers {
    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "1.13.3"
    }
  }
}

resource "local_file" "oauth2_credentials" {
  filename = "/oauth2_credentials_file"
}

data "local_file" "client_certificate" {
  filename = "/home/mini-batch/.kube/config/client.crt"
}

data "local_file" "client_key" {
  filename = "/home/mini-batch/.kube/config/client.key"
}

data "local_file" "cluster_ca_certificate" {
  filename = "/home/mini-batch/.kube/config/ca.crt"
}

provider "kubernetes" {
  load_config_file = false

  host = "https://${var.minikube_host_ip}:8443"

  client_certificate     = data.local_file.client_certificate.content
  client_key             = data.local_file.client_key.content
  cluster_ca_certificate = data.local_file.cluster_ca_certificate.content
}

data "terraform_remote_state" "infra" {
  backend = "gcs"
  config = {
    bucket  = var.tf_state_bucket
    prefix  = "mini-batch/terraform/infra/state"
  }
}

locals {
  db = data.terraform_remote_state.infra.outputs.db
  db_root_password = data.terraform_remote_state.infra.outputs.db_root_password
  batch_logs = data.terraform_remote_state.infra.outputs.batch_logs
  hail_query_storage = data.terraform_remote_state.infra.outputs.hail_query
  root_client_cert = data.terraform_remote_state.infra.outputs.root_client_cert
  docker_root_image = data.terraform_remote_state.infra.outputs.docker_root_image
}

resource "kubernetes_secret" "global_config" {
  metadata {
    name = "global-config"
  }

  data = {
    cloud = "gcp"
    batch_gcp_regions = var.batch_gcp_regions
    batch_logs_storage_uri = "gs://${local.batch_logs.name}"
    query_storage_uri  = "gs://${local.hail_query_storage.name}"
    default_namespace = "default"
    docker_root_image = data.terraform_remote_state.infra.outputs.docker_root_image
    gcp_project = data.terraform_remote_state.infra.outputs.gcp_project
    gcp_region = data.terraform_remote_state.infra.outputs.gcp_region
    gcp_zone = data.terraform_remote_state.infra.outputs.gcp_zone
    docker_prefix = data.terraform_remote_state.infra.outputs.docker_prefix
    internal_ip = var.internal_ip
    ip = var.external_ip
    organization_domain = var.organization_domain
  }
}

resource "kubernetes_cluster_role" "batch" {
  metadata {
    name = "batch"
  }

  rule {
    api_groups = [""]
    resources  = ["secrets", "serviceaccounts"]
    verbs      = ["get", "list"]
  }
}

resource "kubernetes_cluster_role_binding" "batch" {
  metadata {
    name = "batch"
  }
  role_ref {
    kind      = "ClusterRole"
    name      = "batch"
    api_group = "rbac.authorization.k8s.io"
  }
  subject {
    kind      = "ServiceAccount"
    name      = "batch"
    namespace = "default"
  }
}

resource "kubernetes_secret" "auth_oauth2_client_secret" {
  metadata {
    name = "auth-oauth2-client-secret"
  }

  data = {
    "client_secret.json" = file(local_file.oauth2_credentials.filename)
  }
}

resource "kubernetes_secret" "database_server_config" {
  metadata {
    name = "database-server-config"
  }

  data = {
    "server-ca.pem" = local.db.server_ca_cert.0.cert
    "client-cert.pem" = local.root_client_cert.cert
    "client-key.pem" = local.root_client_cert.private_key
    "sql-config.cnf" = <<END
[client]
host=${local.db.ip_address[0].ip_address}
user=root
password=${local.db_root_password.result}
ssl-ca=/sql-config/server-ca.pem
ssl-mode=VERIFY_CA
ssl-cert=/sql-config/client-cert.pem
ssl-key=/sql-config/client-key.pem
END
    "sql-config.json" = <<END
{
    "ssl-cert": "/sql-config/client-cert.pem",
    "ssl-key": "/sql-config/client-key.pem",
    "ssl-ca": "/sql-config/server-ca.pem",
    "ssl-mode": "VERIFY_CA",
    "host": "${local.db.ip_address[0].ip_address}",
    "port": 3306,
    "user": "root",
    "password": "${local.db_root_password.result}",
    "instance": "${local.db.name}",
    "connection_name": "${local.db.connection_name}",
    "docker_root_image": "${local.docker_root_image}"
}
END
  }
}