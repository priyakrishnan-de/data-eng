## High level architecture for load balancing 3 VM's using GKE Cluster

```mermaid
flowchart TB

    %% === VPC & Subnet ===
    subgraph VPC["Custom VPC Network"]
        direction TB
        Subnet["Subnet IP's (Primary range): 10.10.0.0/16, 
        Pods IP's (Secondary range): 10.20.0.0/16, 
        Services IP's (Secondary range): 10.30.0.0/20"]
    end

    %% === GKE Cluster ===
    subgraph GKE["Private GKE Cluster"]
        direction TB
        ControlPlane["GKE Control Plane (Managed by Google)"]
        NodePool["Node Pool (e2-micro nodes) Private IPs"]
        ILB["Internal Load Balancer (GKE Service Type as LoadBalancer)"]

        ControlPlane --> NodePool
        NodePool --> ILB
    end

    %% === Backend VMs ===
    subgraph Backend["Private Compute VMs"]
        direction LR
        VM0["compute-vm-0"]
        VM1["compute-vm-1"]
        VM2["compute-vm-2"]
    end

    %% Relationships
    VPC --- GKE
    VPC --- Backend

    %% Firewall rules annotation
    ILB --- Note1("Load Balancer Firewall rule:Allow GKE Pod CIDR → VMs (TCP:80)")

    %% ILB to VMs traffic flow
    ILB --> VM0
    ILB --> VM1
    ILB --> VM2
```




### Steps followed for creating terraform files - from local
1. Create main.tf and create resources one by one at every step using terraform validate, terraform plan, terraform apply

2. Create output.tf to output the resource detail at every step as applicable

3. Once the resources are created, ensure reusable variables such as project, region, zone, vpc, subnet, vm, vm size etc are moved to variables.tf. 

4. #### kubectl Setup

    *From Google Cloud Shell, Install kubectl if not available for deploying Load balancer*

    1. Run Google Cloud shell from local as Administrator

        `gcloud components install gke-gcloud-auth-plugin`

    2. Verify if installed

        `gke-gcloud-auth-plugin --version`


5. #### Create YAML files for deploying ILB

    service.yaml, deployment.yaml, configmap.yaml in another folder *k8s/lb*

    deployment.yaml - deploys proxy pod "vm-proxy" app with enginx containers 

    service.yaml - Create service for an internal load balancer for deployed app "vm-proxy"

    configmap.yaml - Configuration for three VM's listening on port 80


6. To Deploy the Loadbalancer, provide path where the yaml files are    available:

    `kubectl apply -f C:/Users/OrCon/GitHub/data-eng/k8s/lb/`


7. Updates kubeconfig file with credentials and endpoint to point to kubectl at a specific cluster in GKE.

    `gcloud container clusters get-credentials private-gke-cluster --region=asia-east1`


**Steps followed for verification of resources - Google Cloud SDK**

1. Verify Kubernetes resources:

    `kubectl get deployments`

    `kubectl get pods`

    `kubectl get svc`

2. Verify nodes:

    `kubectl get nodes`

3. Get more details on a node:

    `kubectl describe node <node-name>`

4. Confirm LB and Status:

    `kubectl get svc vm-proxy`

5. Describe LB:

    `kubectl describe svc vm-proxy`

6. To check config map:

    `kubectl describe configmap vm-proxy-conf`

    If ConfigMap is updated, delete and restart the proxy pod:

    `kubectl delete pod -l app=vm-proxy`


### Troubleshooting:

1. Test if load balancer is working inside GKE:

    a. Enter into bash:

    `kubectl run curl-test --rm -it --image=alpine/curl --restart=Never -- sh`

    In case earlier pod is still remaining from previous session, delete it first and then enter bash:

    `kubectl delete pod curl-test`

    b. Inside pod, check if LB is working and round robin message received from the 3 VM's:

    `curl http://<INTERNAL_LB_IP>`

    Expected to get hello message from 3 VM's. Instead if welcome to nginx is displayed, it means ILB + Kubernetes + proxy pod are working fine. But nginx is not able to reach the VM's.

    c. If LB is not working, check directly if VM is responding within bash:

    `curl http://<VM 1 IP>
    curl http://<VM 2 IP>
    curl http://<VM 3 IP>`

    (or)

    c. SSH into one VM and check if it returns Hello message from PowerShell:

    `gcloud compute ssh compute-vm-0 --tunnel-through-iap --zone=asia-east1-c`

    Ping the server (localhost) from within:
    `curl localhost`

    d. If server is not up, manually update and restart VM:

    `sudo apt-get update -y`
    
    `sudo apt-get install -y apache2`
    
    `echo "Hello from compute-vm-0" | sudo tee /var/www/html/index.html`
    
    `sudo systemctl restart apache2`

    Then check if it is listening on port 80:
    `sudo ss -tlnp | grep 80`

    e. If it does not return anything, it means it is still not listening.

    If VM does not have Apache installed even after manual update, it does not have access to get updates from internet. So need to allow for getting updates using NAT config /egress. Include the NAT router and config in main.tf and apply it.

    f. Manually update and restart VM after NAT config and check if VM is listening on 80

    g. Repeat the same step for other two VM's and ensure they are listening on port 80 using either ssh or bash with curl

        Second VM:
        `gcloud compute ssh compute-vm-1 --tunnel-through-iap --zone=asia-east1-c`

        `sudo apt-get update -y`
        
        `sudo apt-get install -y apache2` 
        
        `echo "Hello from compute-vm-1" | sudo tee /var/www/html/index.html` 
        
        `sudo systemctl restart apache2`

        `sudo ss -tlnp | grep 80`

        Third VM:
        `gcloud compute ssh compute-vm-2 --tunnel-through-iap --zone=asia-east1-c`

        `sudo apt-get update -y`
        
        `sudo apt-get install -y apache2` 
        
        `echo "Hello from compute-vm-2" | sudo tee /var/www/html/index.html` 
        
        `sudo systemctl restart apache2`

        `sudo ss -tlnp | grep 80`


2. If VM's are responding, Check proxy pod logs if there are issues with ILB reaching VM's:

    `kubectl logs deploy/vm-proxy`


3. Check if firewall rule was created for GKE prod to reach VM's on port 80:

    `gcloud compute firewall-rules list --filter="network=private-vpc"`

4. If Firewall rule not available, To create firewall rule for allowing requests from Secondary CIDR (services) to VM's on port 80:

    `gcloud compute firewall-rules create allow-gke-to-vms --network=private-vpc --allow=tcp:80 --source-ranges=10.20.0.0/16 --target-tags=backend-vms`


5. Ensure back end VM's also have the same network tag

    Verify tag:

    `gcloud compute instances describe compute-vm-0 --format="get(tags.items) --zone="asia-east1-c"`

    Update tag:

    `gcloud compute instances add-tags compute-vm-0 --tags=backend-vms`
    `gcloud compute instances add-tags compute-vm-1 --tags=backend-vms`
    `gcloud compute instances add-tags compute-vm-2 --tags=backend-vms`


6. To resize the clusters and make the number of nodes as zero:

    `gcloud container clusters resize private-gke-cluster --num-nodes=0 --region=asia-east1`
