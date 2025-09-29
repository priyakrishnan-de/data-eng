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
  credentials = "../../../key-petal-471015-g3-9c54a8030d5e.json"
  project     = "key-petal-471015-g3"
  region      = "asia-east1"
}


resource "google_compute_network" "main_vpc" {
  name = "private-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main_subnet" {
  name          = "private-subnet"
  ip_cidr_range = "10.10.0.0/16"
  region        = "asia-east1"
  network       = google_compute_network.main_vpc.id
  private_ip_google_access = true
}

resource "google_compute_instance" "compute" {
  count        = 3
  name         = "compute-vm-${count.index}"
  machine_type = "e2-micro"
  zone         = "asia-east1-c"

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
    apt-get update
    apt-get install -y apache2
    echo "Hello from VM-${count.index}" > /var/www/html/index.html
    systemctl restart apache2
  EOT
}

resource "google_container_cluster" "gke_cluster" {
  name       = "private-gke-cluster"
  location   = "asia-east1"
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

  ip_allocation_policy {}
}

resource "google_container_node_pool" "default_nodes" {
  name       = "default-pool"
  cluster    = google_container_cluster.gke_cluster.name
  location   = google_container_cluster.gke_cluster.location
  
  node_count = 1

  node_config {
    machine_type = "e2-small"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    #network_tags = ["gke-nodes"]
  }
}




