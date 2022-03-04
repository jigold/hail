output "network" {
  value = google_compute_network.default
}

output "version" {
  value = 1
}

output "gcp_region" {
  value = var.gcp_region
}

output "gcp_project" {
  value = var.gcp_project
}

output "gcp_zone" {
  value = var.gcp_zone
}

output "gcp_location" {
  value = var.gcp_location
}
