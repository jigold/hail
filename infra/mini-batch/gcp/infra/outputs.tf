output "auth_gsa" {
  value = module.auth_gsa
}

output "batch_gsa" {
  value = module.batch_gsa
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
  value = string
}

output "gcp_region" {
  value = string
}

output "gcp_zone" {
  value = string
}

output "db" {
  value = google_sql_database_instance.db
}

output "db_root_password" {
  value = random_password.db_root_password
}

output "root_client_cert" {
  value = google_sql_ssl_cert.root_client_cert
}
