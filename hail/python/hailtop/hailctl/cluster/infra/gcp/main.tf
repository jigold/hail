terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.48.0"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "1.13.3"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "3.1.0"
    }
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

data "google_client_openid_userinfo" "me" {}

locals {
  username = split("@", data.google_client_openid_userinfo.me.email)[0]
}

provider "tls" {
  // no config needed
}

resource "tls_private_key" "ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "local_file" "ssh_private_key_pem" {
  content         = tls_private_key.ssh.private_key_pem
  filename        = ".ssh/gce-${var.cluster_name}"
  file_permission = "0600"
}

# Setup batch, auth service accounts
# Setup permissions
# Setup bucket for batch logs
# Setup GCR

resource "google_compute_network" "default" {
  name = "${var.cluster_name}-default"
}

data "google_compute_subnetwork" "default_region" {
  name = "${var.cluster_name}-default"
  region = var.gcp_region
  depends_on = [google_compute_network.default]
}

resource "google_compute_firewall" "allow_ssh" {
  name          = "allow-ssh"
  network       = google_compute_network.default.name
  target_tags   = ["allow-ssh", "batch2-agent"] // this targets our tagged VM
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "default_allow_internal" {
  name    = "default-allow-internal"
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

resource "google_service_account" "default" {
  account_id   = "service_account_id"
  display_name = "Service Account"
}

resource "google_compute_instance" "orchestrator" {
  name         = "test"
  machine_type = "n1-standard-2"
  zone         = var.gcp_zone

  tags = ["foo", "bar", "allow-ssh"]

  boot_disk {
    auto_delete = True

    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-minimal-2004-focal-v20220203"
      size = 30
      type = "pd-ssd"
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    foo = "bar"
    ssh-keys = "${split("@", data.google_client_openid_userinfo.me.email)[0]}:${tls_private_key.ssh.public_key_openssh}"
  }

  metadata_startup_script = "echo hi > /test.txt"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = google_service_account.default.email
    scopes = ["cloud-platform"]
  }

  scheduling {
    automatic_restart = False
  }

  connection {
    type     = "ssh"
    user     = local.username
    host     = self.network_interface.0.access_config.0.nat_ip
    private_key = tls_private_key.ssh.private_key_pem
  }

  # Wait for ssh to be available
  provisioner "remote-exec" {
    inline = ["true"]
  }

  provisioner "local-exec" {
    command = <<CMD
      ssh ${local.username}@${self.network_interface.0.access_config.0.nat_ip} "while [ ! -f /test.txt ]; do echo 'Waiting for /test.txt ...'; sleep 1; done" \
        && scp -i .ssh/gce-${var.cluster_name} ${local.username}@${self.network_interface.0.access_config.0.nat_ip}:/test.txt /tmp/test.txt
CMD
  }
}

resource "local_file" "kubeconfig" {
  value = file("/tmp/test.txt")
  filename = "/tmp/.kube/${var.cluster_name}.config"
  depends_on = [google_compute_instance.orchestrator]
}

# Kubernetes setup

provider "kubernetes" {
  config_path    = local_file.kubeconfig.filename
}

resource "kubernetes_namespace" "example" {
  metadata {
    name = "test-ns"
  }
}

resource "null_resource" "bootstrap" {
  connection {
    type     = "ssh"
    user     = local.username
    host     = google_compute_instance.orchestrator.network_interface.0.access_config.0.nat_ip
    private_key = tls_private_key.ssh.private_key_pem
  }

  # Wait for ssh to be available
  provisioner "remote-exec" {
    inline = ["echo hello"]
  }

  depends_on = [kubernetes_namespace.example]
}
