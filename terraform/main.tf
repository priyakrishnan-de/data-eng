
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

/*
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


resource "google_bigquery_dataset" "priya_dataset" {
  dataset_id  = var.bq_dataset_name
  location    = var.location
  description = "Dataset for GCP Training"
}

resource "google_cloud_run_service" "service" {
  name     = var.cloud_run_service_name
  location = var.location
  project  = var.project

  template {
    spec {
      containers {
        image = "asia.gcr.io/key-petal-471015-g3/data-export-service"   # replace with your container image
        resources {
          limits = {
            memory = "256Mi"
          }
        }

        # optional env vars
        # env {
        #   name  = "ENV"
        #   value = "dev"
        # }
      }

      container_concurrency = 80
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}
*/

resource "google_cloud_scheduler_job" "call_cloudrun" {
  name        = "trigger-cloudrun"
  description = "Trigger Cloud Run from Cloud Scheduler"
  schedule    = "*/5 * * * *"   # every 5 minutes (cron format)
  time_zone   = "Asia/Kolkata"

  http_target {
    http_method = "GET"
    uri         = google_cloud_run_service.service.status[0].url
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}

resource "google_service_account" "scheduler_sa" {
  account_id   = "scheduler-invoker"
  display_name = "Scheduler Invoker"
}

# Give Scheduler permission to invoke Cloud Run
resource "google_cloud_run_service_iam_member" "invoker" {
  location = google_cloud_run_service.service.location
  project  = var.project
  service  = google_cloud_run_service.service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}





