variable "credentials" {
  description = "Path to the Google Cloud service account credentials JSON file"
  type        = string
  default     = "../../../../key-petal-471015-g3-9c54a8030d5e.json"
}

variable "project_id" {
  description = "GCP project ID where resources will be created"
  type        = string
  default     = "key-petal-471015-g3"
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "asia-east1"
}

variable "zone" {
  description = "GCP zone for backend VMs and node pool"
  type        = string
  default     = "asia-east1-c"
}

variable "name_prefix" { default = "ingest" }

# Networking
variable "vpc_name" { default = "ingest-vpc" }
variable "subnet_name" { default = "ingest-subnet" }
variable "subnet_cidr" { default = "10.50.0.0/16" }
variable "vpc_connector_cidr" { default = "10.50.8.0/28" }

# Buckets
variable "transient_bucket_name" {}
variable "target_bucket_name" {}
variable "transient_bucket_ttl_days" { default = 7 }

# Vendor IAM
variable "vendor_principal" {
  description = "External vendor user/service account email"
  default = "serviceAccount:vendor-sa@key-petal-471015-g3.iam.gserviceaccount.com"
}

# DB
variable "pg_tier" { default = "db-f1-micro" }
variable "pg_db_name" { default = "filelog" }
variable "pg_user" { default = "filelogger" }
variable "pg_password" { sensitive = true }

# Cloud Run
variable "fileproc_image" {
  description = "File processing container image URI"
}

# BigQuery
/*
variable "bq_gold_dataset" { default = "gold_users" }

variable "bq_gold_latest_schema" {
  type = any
  default = [
    { name = "UserID", type = "STRING", mode = "REQUIRED" },
    { name = "UserName", type = "STRING", mode = "NULLABLE" },
    { name = "LastUpdatedAt", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "TotalPoints", type = "INTEGER", mode = "NULLABLE" }
  ]
}

variable "bq_gold_history_schema" {
  type = any
  default = [
    { name = "UserID", type = "STRING", mode = "REQUIRED" },
    { name = "EventDate", type = "DATE", mode = "REQUIRED" },
    { name = "TotalPoints", type = "INTEGER", mode = "NULLABLE" },
    { name = "DurationSeconds", type = "INTEGER", mode = "NULLABLE" }
  ]
}
*/