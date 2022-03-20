output "auth_gsa" {
  value = module.auth_gsa
  sensitive = true
}

output "batch_gsa" {
  value = module.batch_gsa
  sensitive = true
}

output "batch_logs" {
  value = module.batch_logs
}

output "hail_query" {
  value = module.hail_query
}

output "docker_prefix" {
  value = local.docker_prefix
}

output "docker_root_image" {
  value = local.docker_root_image
}

output "gcp_project" {
  value = local.gcp_project
}

output "gcp_region" {
  value = local.gcp_region
}

output "gcp_zone" {
  value = local.gcp_zone
}

output "db" {
  value = google_sql_database_instance.db
  sensitive = true
}

output "db_root_password" {
  value = random_password.db_root_password
  sensitive = true
}

output "root_client_cert" {
  value = google_sql_ssl_cert.root_client_cert
  sensitive = true
}
