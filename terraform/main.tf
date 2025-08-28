
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.48.0"
    }
  }
}


provider "google" {
  # Configuration options
  credentials = file(var.credentials)
  project     = var.project
  region      = var.location
}

resource "google_storage_bucket" "priya_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "priya_demo_dataset" {
  dataset_id  = var.bq_dataset_name
  location    = var.location
  description = "Demo dataset for Priya DE GCP"
}
