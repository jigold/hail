terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.48.0"
    }
  }

  backend "gcs" {
#    bucket  = # Set with -backend-config "bucket=BUCKET"
    prefix  = "mini-batch/terraform/driver/state"
  }
}

provider "google" {
  project = var.gcp_project
  region = var.gcp_region
  zone = var.gcp_zone
}

provider "google-beta" {
  project = var.gcp_project
  region = var.gcp_region
  zone = var.gcp_zone
}

resource "google_compute_network" "default" {
  name = "mini-batch-${var.cluster_name}"
}

data "google_compute_subnetwork" "default_region" {
  name = "mini-batch-${var.cluster_name}"
  region = var.gcp_region
  depends_on = [google_compute_network.default]
}

resource "google_compute_firewall" "allow_ssh" {
  name          = "mb-${var.cluster_name}-allow-ssh"
  network       = google_compute_network.default.name
  target_tags   = ["mini-batch", "batch2-agent"]
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "default_allow_internal" {
  name    = "mb-${var.cluster_name}-default-allow-internal"
  network = google_compute_network.default.name

  priority = 65534

  source_ranges = ["10.128.0.0/9"]

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }
}

resource "google_service_account" "mini_batch_sa" {
  account_id   = "mini-batch-${var.cluster_name}"
  display_name = "mini-batch-${var.cluster_name}"
}

resource "google_project_iam_member" "mini_batch_owner" {
  role = "roles/owner"
  member = "serviceAccount:${google_service_account.mini_batch_sa.email}"
}

resource "google_compute_instance" "driver" {
  name         = "mini-batch-${var.cluster_name}"
  machine_type = "${var.machine_family}-${var.machine_type}-${var.n_cores}"
  zone         = var.gcp_zone

  tags = ["mini-batch", "allow-ssh"]

  boot_disk {
    auto_delete = "true"

    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-minimal-2004-focal-v20220203"
      size = var.boot_disk_size
      type = "pd-ssd"
    }
  }

  network_interface {
    network = google_compute_network.default.name

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    oauth2_credentials_file = var.oauth2_credentials_file
    bucket_storage_class = var.bucket_storage_class
    bucket_location = var.bucket_location
    tf_state_bucket = var.tf_state_bucket
    organization_domain = var.organization_domain
    username = var.username
    email = var.email
    db_cores = var.db_cores
    db_memory = var.db_memory
    region = var.gcp_region
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = google_service_account.mini_batch_sa.email
    scopes = ["cloud-platform"]
  }

  scheduling {
    automatic_restart = "false"
  }

  metadata_startup_script = <<EOT
#! /bin/bash
set -ex

sudo apt-get update
sudo apt-get install -y git

git clone https://github.com/${var.repo}.git
cd hail/
git checkout "${var.commit}"
cd infra/mini-batch/gcp/

sudo useradd -m minibatch
sudo usermod -aG sudo minibatch

sudo - minibatch -c "nohup sh bootstrap.sh > /bootstrap.log &"
EOT
}
