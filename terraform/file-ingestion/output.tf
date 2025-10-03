output "vpc_name" {
  value = google_compute_network.main.name
}

output "subnet_self_link" {
  value = google_compute_subnetwork.main.self_link
}

output "cloud_sql_private_ip" {
  value = google_sql_database_instance.pg.private_ip_address
}

output "serverless_vpc_connector" {
  value = google_vpc_access_connector.run_connector.name
}

output "cloud_run_url" {
  value = google_cloud_run_v2_service.fileproc.uri
}

output "transient_bucket" {
  value = google_storage_bucket.transient.name
}

output "target_bucket" {
  value = google_storage_bucket.target.name
}

/*
output "bigquery_dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}
*/
