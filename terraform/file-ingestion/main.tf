############################################################
# Terraform + Providers
############################################################
terraform {
  required_version = ">= 1.4.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.31"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.31"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

############################################################
# Enable Required APIs
############################################################
locals {
  apis = [
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "run.googleapis.com",
    "eventarc.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "compute.googleapis.com",
    "artifactregistry.googleapis.com",
    "sqladmin.googleapis.com",
    "bigquery.googleapis.com",
    "vpcaccess.googleapis.com",
    "servicenetworking.googleapis.com",
    "secretmanager.googleapis.com"
  ]
}

resource "google_project_service" "services" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

############################################################
# Networking - VPC + Subnet + PSA for Cloud SQL Private IP
############################################################
resource "google_compute_network" "main" {
  name                    = var.vpc_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main" {
  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network                  = google_compute_network.main.id
  private_ip_google_access = true
}

resource "google_compute_global_address" "sql_psa" {
  name          = "${var.name_prefix}-psa-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
}

resource "google_service_networking_connection" "psa" {
  network                 = google_compute_network.main.id
  service                 = "services/servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.sql_psa.name]
  depends_on              = [google_project_service.services]
}

############################################################
# Serverless VPC Connector (Cloud Run → Cloud SQL private IP)
############################################################
resource "google_vpc_access_connector" "run_connector" {
  name          = "${var.name_prefix}-vpc-connector"
  region        = var.region
  network       = google_compute_network.main.name
  ip_cidr_range = var.vpc_connector_cidr
}

############################################################
# GCS Buckets (Transient + Target)
############################################################
resource "google_storage_bucket" "transient" {
  name                        = var.transient_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = var.transient_bucket_ttl_days }
  }
}

resource "google_storage_bucket" "target" {
  name                        = var.target_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

############################################################
# Service Accounts + IAM (Vendor SA1→SA2 impersonation)
############################################################
resource "google_service_account" "sa_dropper" {
  account_id   = "sa-dropper"
  display_name = "SA2 - object creator only"
}

resource "google_storage_bucket_iam_member" "dropper_object_creator" {
  bucket = google_storage_bucket.transient.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.sa_dropper.email}"
}

resource "google_service_account" "sa_impersonation" {
  account_id   = "sa-impersonation"
  display_name = "SA1 - impersonation"
}

resource "google_service_account_iam_member" "sa1_token_sa2" {
  service_account_id = google_service_account.sa_dropper.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.sa_impersonation.email}"
}

resource "google_service_account_iam_member" "vendor_token_sa1" {
  service_account_id = google_service_account.sa_impersonation.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = var.vendor_principal
}

############################################################
# Secrets Manager for DB credentials
############################################################
resource "google_secret_manager_secret" "db_host" {
  secret_id = "${var.name_prefix}-db-host"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "db_name" {
  secret_id = "${var.name_prefix}-db-name"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "db_user" {
  secret_id = "${var.name_prefix}-db-user"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "db_password" {
  secret_id = "${var.name_prefix}-db-password"
  replication {
    auto {}
  }
}

############################################################
# Cloud SQL (Postgres) - Private IP
############################################################
resource "google_sql_database_instance" "pg" {
  name             = "${var.name_prefix}-pg"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = var.pg_tier
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.main.id
    }
    backup_configuration { enabled = true }
  }

  depends_on = [google_service_networking_connection.psa]
}

resource "google_sql_database" "pg_db" {
  name     = var.pg_db_name
  instance = google_sql_database_instance.pg.name
}

resource "google_sql_user" "pg_user" {
  instance = google_sql_database_instance.pg.name
  name     = var.pg_user
  password = var.pg_password
}

# Secret values
resource "google_secret_manager_secret_version" "db_host_v" {
  secret      = google_secret_manager_secret.db_host.id
  secret_data = google_sql_database_instance.pg.private_ip_address
}

resource "google_secret_manager_secret_version" "db_name_v" {
  secret      = google_secret_manager_secret.db_name.id
  secret_data = var.pg_db_name
}

resource "google_secret_manager_secret_version" "db_user_v" {
  secret      = google_secret_manager_secret.db_user.id
  secret_data = var.pg_user
}

resource "google_secret_manager_secret_version" "db_password_v" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = var.pg_password
}

############################################################
# Cloud Run (File Processor) + EventArc
############################################################
resource "google_service_account" "sa_run" {
  account_id   = "sa-fileproc-run"
  display_name = "Cloud Run file processor"
}

# Secret access for Cloud Run
resource "google_secret_manager_secret_iam_member" "run_secret_access" {
  for_each = {
    host = google_secret_manager_secret.db_host.id
    name = google_secret_manager_secret.db_name.id
    user = google_secret_manager_secret.db_user.id
    pass = google_secret_manager_secret.db_password.id
  }
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.sa_run.email}"
}

resource "google_cloud_run_v2_service" "fileproc" {
  name     = "${var.name_prefix}-fileproc"
  location = var.region

  template {
    service_account = google_service_account.sa_run.email

    vpc_access {
      connector = google_vpc_access_connector.run_connector.id
      egress    = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = var.fileproc_image

      env {
        name  = "TRANSIENT_BUCKET"
        value = google_storage_bucket.transient.name
      }

      env {
        name  = "TARGET_BUCKET"
        value = google_storage_bucket.target.name
      }

      env {
        name = "PG_HOST"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_host.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "PG_DB"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_name.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "PG_USER"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_user.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "PG_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password.secret_id
            version = "latest"
          }
        }
      }
    }



    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
   
  }
}


resource "google_service_account" "sa_eventarc" {
  account_id   = "sa-eventarc"
  display_name = "EventArc Trigger SA"
}

resource "google_project_iam_member" "eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.sa_eventarc.email}"
}

resource "google_project_iam_member" "eventarc_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.sa_eventarc.email}"
}

resource "google_cloud_run_v2_service_iam_member" "run_invoker_eventarc" {
  name     = google_cloud_run_v2_service.fileproc.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.sa_eventarc.email}"
}

resource "google_eventarc_trigger" "gcs_to_run" {
  name     = "${var.name_prefix}-gcs-finalize"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.transient.name
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.fileproc.name
      region  = var.region
      path    = "/"
    }
  }

  service_account = google_service_account.sa_eventarc.email
}

############################################################
# BigQuery (Gold tables)
############################################################
/*
resource "google_bigquery_dataset" "gold" {
  dataset_id                 = var.bq_gold_dataset
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "gold_latest" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "gold_latest"
  schema     = jsonencode(var.bq_gold_latest_schema)
}

resource "google_bigquery_table" "gold_history" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "gold_history"
  schema     = jsonencode(var.bq_gold_history_schema)
}
*/