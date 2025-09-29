variable "credentials" {
  description = "Path to the Google Cloud service account credentials JSON file"
  type        = string
  default     = "../../../key-petal-471015-g3-9c54a8030d5e.json"
}

variable "project" {
  description = "Google Cloud Project"
  type        = string
  default     = "key-petal-471015-g3"
}

variable "location" {
  description = "Projection location for the resources"
  type        = string
  default     = "asia-east1"
}


variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  type        = string
  default     = "priya_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
  default     = "priya_bucket"
}

variable "gcs_storage_class" {
  description = "Storage class for Google Cloud Storage bucket"
  type        = string
  default     = "STANDARD"
}


variable "cloud_run_service_name" {
  type    = string
  default = "priya-cloudrun"
}
