
output "gcs_bucket_name" {
  description = "The name of the Google Cloud Storage bucket"
  value       = google_storage_bucket.priya_bucket.name
}

output "gcs_bucket_url" {
  description = "The URL of the Google Cloud Storage bucket"
  value       = "gs://${google_storage_bucket.priya_bucket.name}"
}

output "bq_dataset_id" {
  description = "The ID of the BigQuery dataset"
  value       = google_bigquery_dataset.priya_dataset.dataset_id
}

output "bq_dataset_project" {
  description = "The project where the BigQuery dataset resides"
  value       = google_bigquery_dataset.priya_dataset.project
}

output "bq_dataset_location" {
  description = "The location of the BigQuery dataset"
  value       = google_bigquery_dataset.priya_dataset.location
}


output "cloud_run_service_url" {
  value       = google_cloud_run_service.service.status[0].url
  description = "Public HTTPS URL of the Cloud Run service"
}