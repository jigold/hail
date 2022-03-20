terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.48.0"
    }
  }

  backend "gcs" {
    #    bucket  = # Set with -backend-config "bucket=BUCKET"
    prefix  = "mini-batch/terraform/infra/state"
  }
}

data "terraform_remote_state" "driver" {
  backend = "gcs"
  config = {
    bucket  = var.tf_state_bucket
    prefix  = "mini-batch/terraform/driver/state"
  }
}

locals {
  gcp_region = data.terraform_remote_state.driver.gcp_region
  gcp_project = data.terraform_remote_state.driver.gcp_project
  gcp_zone = data.terraform_remote_state.driver.gcp_zone
  gcp_location = data.terraform_remote_state.driver.gcp_location
  network = data.terraform_remote_state.driver.network
  docker_prefix = "${local.gcp_region}-docker.pkg.dev/${local.gcp_project}/hail"
  docker_root_image = "${local.docker_prefix}/ubuntu:20.04"
}

provider "google" {
  project = local.gcp_project
  region = local.gcp_region
  zone = local.gcp_zone
}

provider "google-beta" {
  project = local.gcp_project
  region = local.gcp_region
  zone = local.gcp_zone
}

data "google_client_config" "provider" {}

resource "random_id" "db_name_suffix" {
  byte_length = 4
}

# Without this, I get:
# Error: Error, failed to create instance because the network doesn't have at least
# 1 private services connection. Please see
# https://cloud.google.com/sql/docs/mysql/private-ip#network_requirements
# for how to create this connection.
resource "google_compute_global_address" "google_managed_services_default" {
  name = "google-managed-services-default"
  purpose = "VPC_PEERING"
  address_type = "INTERNAL"
  prefix_length = 16
  network = local.network.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network = local.network.id
  service = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.google_managed_services_default.name]
}

resource "google_compute_network_peering_routes_config" "private_vpc_peering_config" {
  peering = google_service_networking_connection.private_vpc_connection.peering
  network = local.network.name
  import_custom_routes = true
  export_custom_routes = true
}

resource "google_sql_database_instance" "db" {
  name = "db-${random_id.db_name_suffix.hex}"
  database_version = "MYSQL_5_7"
  region = local.gcp_region

  depends_on = [google_service_networking_connection.private_vpc_connection]

  settings {
    # https://cloud.google.com/sql/docs/mysql/instance-settings
    tier = "db-custom-${var.db_cores}-${var.db_memory}"

    ip_configuration {
      ipv4_enabled = false
      private_network = local.network.id
      require_ssl = true
    }
  }
}

resource "google_sql_ssl_cert" "root_client_cert" {
  common_name = "root-client-cert"
  instance = google_sql_database_instance.db.name
}

resource "random_password" "db_root_password" {
  length = 22
}

resource "google_sql_user" "db_root" {
  name = "root"
  instance = google_sql_database_instance.db.name
  password = random_password.db_root_password.result
}

# FIXME: Does this delete the entire registry or just the repository
resource "google_artifact_registry_repository" "repository" {
  provider = google-beta
  format = "DOCKER"
  repository_id = "hail"
  location = local.gcp_location  # FIXME: Should this just be the region or "US"
}

resource "google_artifact_registry_repository_iam_member" "artifact_registry_batch_agent_viewer" {
  provider = google-beta
  repository = google_artifact_registry_repository.repository.name
  location = local.gcp_location
  role = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.batch_agent.email}"
}

module "batch_logs" {
  source        = "../../../gcp/gcs_bucket"
  short_name    = "batch-logs"
  location      = var.bucket_location
  storage_class = var.bucket_storage_class
}

module "hail_query" {
  source        = "../../../gcp/gcs_bucket"
  short_name    = "hail-query"
  location      = var.bucket_location
  storage_class = var.bucket_storage_class
}

module "auth_gsa" {
  source = "./service_account"
  name = "auth"
  iam_roles = [
    "iam.serviceAccountAdmin",
    "iam.serviceAccountKeyAdmin",
  ]
  gcp_project = local.gcp_project
}

module "batch_gsa" {
  source = "./service_account"
  name = "batch"
  iam_roles = [
    "compute.instanceAdmin.v1",
    "iam.serviceAccountUser",
    "logging.viewer",
    "storage.admin",
  ]
  gcp_project = local.gcp_project
}

resource "google_storage_bucket_iam_member" "batch_hail_query_bucket_storage_viewer" {
  bucket = module.hail_query.name
  role = "roles/storage.objectViewer"
  member = "serviceAccount:${module.batch_gsa.email}"
}

resource "google_service_account" "batch_agent" {
  account_id = "batch2-agent"
}

resource "google_project_iam_member" "batch_agent_iam_member" {
  project = local.gcp_project

  for_each = toset([
    "compute.instanceAdmin.v1",
    "iam.serviceAccountUser",
    "logging.logWriter",
    "storage.objectCreator",
    "storage.objectViewer",
  ])

  role = "roles/${each.key}"
  member = "serviceAccount:${google_service_account.batch_agent.email}"
}
