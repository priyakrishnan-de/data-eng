############################
# Project and Region
############################
variable "credentials" {
  description = "Path to the Google Cloud service account credentials JSON file"
  type        = string
  default     = "../../../../key-petal-471015-g3-9c54a8030d5e.json"
}

variable "project_id" {
  description = "GCP project ID where resources will be created"
  type        = string
  default = "key-petal-471015-g3"
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

############################
# VPC and Subnet
############################
variable "vpc_name" {
  description = "Name of the custom VPC network"
  type        = string
  default     = "private-vpc"
}

variable "subnet_name" {
  description = "Name of the primary subnet"
  type        = string
  default     = "private-subnet"
}

variable "subnet_cidr" {
  description = "Primary CIDR range for subnet"
  type        = string
  default     = "10.10.0.0/16"
}

variable "pods_secondary_cidr" {
  description = "Secondary CIDR range for GKE Pods"
  type        = string
  default     = "10.20.0.0/16"
}

variable "services_secondary_cidr" {
  description = "Secondary CIDR range for GKE Services"
  type        = string
  default     = "10.30.0.0/20"
}

############################
# GKE Cluster
############################
variable "gke_cluster_name" {
  description = "Name of the GKE cluster"
  type        = string
  default     = "private-gke-cluster"
}

variable "gke_node_count" {
  description = "Number of nodes in the GKE node pool"
  type        = number
  default     = 3
}

variable "gke_machine_type" {
  description = "Machine type for GKE nodes"
  type        = string
  default     = "e2-small"
}

############################
# Backend VMs
############################
variable "backend_vm_count" {
  description = "Number of backend VMs to create"
  type        = number
  default     = 3
}

variable "backend_vm_name_prefix" {
  description = "Prefix for backend VM names"
  type        = string
  default     = "compute-vm"
}

variable "backend_vm_machine_type" {
  description = "Machine type for backend VMs"
  type        = string
  default     = "e2-micro"
}

variable "backend_vm_tags" {
  description = "Network tags for backend VMs (used for firewall rules)"
  type        = list(string)
  default     = ["backend-vms"]
}

