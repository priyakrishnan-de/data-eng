############################################################
# 1. PROVIDERS & PROJECT SETUP
############################################################
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
  project     = var.project_id
  region      = var.region
  zone = var.zone
}

############################################################
# 2. VPC NETWORK & SUBNET
############################################################
resource "google_compute_network" "main_vpc" {
  name = var.vpc_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main_subnet" {
  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_cidr
  region                   = var.region
  network       = google_compute_network.main_vpc.id
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods-range"
    ip_cidr_range = var.pods_secondary_cidr
  }

  secondary_ip_range {
    range_name    = "services-range"
    ip_cidr_range = var.services_secondary_cidr
  }
}

############################################################
# 3. FIREWALL RULES
############################################################
# Allow GKE pods to talk to backend VMs on TCP:80
resource "google_compute_firewall" "allow_gke_to_vms" {
  name    = "allow-gke-to-vms"
  network = google_compute_network.main_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }

  source_ranges = [var.pods_secondary_cidr]
  target_tags   = var.backend_vm_tags
}

# (Optional) Allow IAP SSH for backend VMs
resource "google_compute_firewall" "iap_ssh" {
  name    = "allow-iap-ssh"
  network = google_compute_network.main_vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"] # IAP range
  target_tags   = var.backend_vm_tags
}

############################################################
# 4. BACKEND VMs (PRIVATE ONLY)
############################################################

resource "google_compute_instance" "backend" {
  count        = var.backend_vm_count
  name         = "${var.backend_vm_name_prefix}-${count.index}"
  machine_type = var.backend_vm_machine_type
  zone         = var.zone
  tags         = var.backend_vm_tags

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    subnetwork         = google_compute_subnetwork.main_subnet.name
    #access config commented to remove external IP and can be connected only privately 
    #access_config {}
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    apt-get update -y
    apt-get install -y apache2
    echo "Hello from VM-${count.index}" > /var/www/html/index.html
    systemctl enable apache2
    systemctl restart apache2
  EOT
}

############################################################
# 5. GKE CLUSTER (PRIVATE, VPC-NATIVE)
############################################################

resource "google_container_cluster" "gke_cluster" {
  name     = var.gke_cluster_name
  location = var.region
  network    = google_compute_network.main_vpc.name
  subnetwork = google_compute_subnetwork.main_subnet.name

  networking_mode = "VPC_NATIVE"
  remove_default_node_pool = true
  initial_node_count = 1

  deletion_protection = false

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods-range"
    services_secondary_range_name = "services-range"
  }
}

############################################################
# 6. GKE NODE POOL
############################################################
resource "google_container_node_pool" "default_nodes" {
  name       = "node-pool"
  cluster    = google_container_cluster.gke_cluster.name
  location   = google_container_cluster.gke_cluster.location
  node_count = var.gke_node_count

  #node_count = 1

  node_config {
    machine_type = "e2-micro"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    tags = ["gke-nodes"]
  }
}

############################################################
# 7. DATA SOURCES (for outputs)
############################################################
data "google_client_config" "default" {}

