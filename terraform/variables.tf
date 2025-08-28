variable "credentials" {
  description = "Path to the Google Cloud service account credentials JSON file"
  type        = string
  default     = "./priya-de-gcp.json"
}


variable "project" {
  description = "Google Cloud Project"
  type        = string
  default     = "priya-de-gcp"
}

variable "location" {
  description = "Projection location for the resources"
  type        = string
  default     = "asia-south1"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  type        = string
  default     = "priya_demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  type        = string
  default     = "priya-de-gcp-bucket"
}

variable "gcs_storage_class" {
  description = "Storage class for Google Cloud Storage bucket"
  type        = string
  default     = "STANDARD"
}