############################
# VPC and Subnet Outputs
############################
output "vpc_name" {
  description = "Name of the created VPC"
  value       = google_compute_network.main_vpc.name
}

output "subnet_self_link" {
  description = "Self link of the created subnet"
  value       = google_compute_subnetwork.main_subnet.self_link
}

############################
# Backend VMs Outputs
############################
output "backend_vm_internal_ips" {
  description = "List of internal IP addresses of backend VMs"
  value       = [for vm in google_compute_instance.backend : vm.network_interface[0].network_ip]
}

output "backend_vm_names" {
  description = "Names of backend VMs"
  value       = [for vm in google_compute_instance.backend : vm.name]
}

############################
# GKE Outputs
############################
output "gke_cluster_name" {
  description = "Name of the GKE cluster"
  value       = google_container_cluster.gke_cluster.name
}

output "gke_cluster_endpoint" {
  description = "Endpoint of the GKE control plane"
  value       = google_container_cluster.gke_cluster.endpoint
}

output "gke_node_pool_nodes" {
  description = "Names of the GKE node pool VMs"
  value       = google_container_node_pool.default_nodes.instance_group_urls
}

############################
# Internal Load Balancer (Service)
############################
# If you use kubernetes_manifest or manually deploy ILB, you can output this separately later.
