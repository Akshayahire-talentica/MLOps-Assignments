# 🚀 Minimal EKS Cluster - Quick Start Guide

Complete guide to deploy a cost-optimized EKS cluster for MLOps POC.

**Cost: $85.51/month** (52% cheaper than full EKS setup)

---

## ✅ Prerequisites

### 1. Install Required Tools

```bash
# Check if installed
eksctl version   # Need v0.150.0 or later
kubectl version  # Need v1.28 or later
aws --version    # Need v2.x

# Install if needed (macOS)
brew install eksctl kubectl awscli
```

### 2. Configure AWS Credentials

```bash
# Check current credentials
aws sts get-caller-identity

# Should show your AWS account ID: 202164733310
```

### 3. Verify Existing Resources

```bash
# Check ECR repositories (should already exist)
aws ecr describe-repositories --region ap-south-1 | grep repositoryName

# Expected output:
# - mlops-api
# - mlops-model-v2
# - mlops-router
# - mlops-mlflow

# Check S3 bucket (should already exist)
aws s3 ls s3://mlops-movielens-data/

# Expected: movies.dat, ratings.dat, users.dat
```

---

## 🎬 Step-by-Step Deployment

### Step 1: Create EKS Cluster (15-20 minutes)

```bash
cd /Users/tohids/Downloads/MLOps-POC-1-main./scripts/aws

# Option A: Create with On-Demand instance ($85.51/month)
./create_minimal_eks_cluster.sh

# Option B: Create with Spot instance ($80.26/month - can be interrupted)
./create_minimal_eks_cluster.sh --spot

# Option C: Custom cluster name
./create_minimal_eks_cluster.sh --cluster-name my-mlops-poc
```

**What happens:**
1. Creates EKS control plane (~5 minutes)
2. Creates VPC with public subnet
3. Launches 1× t3a.small worker node
4. Configures kubectl automatically
5. Verifies cluster is ready

**Expected output:**
```
✓ Cluster created successfully!
Name:              mlops-poc-cluster
Region:            ap-south-1
Kubernetes:        1.31
Worker Nodes:      1
Worker Public IP:  13.235.xxx.xxx
```

**Cost checkpoint:** ✅ EKS Control Plane activated ($73/month)

---

### Step 2: Verify Cluster (2 minutes)

```bash
# Check cluster status
kubectl cluster-info

# Check node
kubectl get nodes
# Should show 1 node in Ready state

# Check system pods
kubectl get pods --all-namespaces
# Should show coredns, kube-proxy, vpc-cni pods running

# Get worker node IP (save this!)
WORKER_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
echo "Worker Node IP: $WORKER_IP"
```

---

### Step 3: Deploy MLOps Services (5-10 minutes)

```bash
# Deploy all services including Prometheus
./deploy_minimal_services.sh

# Or skip Prometheus to save 256MB RAM
./deploy_minimal_services.sh --skip-monitoring

# With custom ECR and image tag
./deploy_minimal_services.sh \
  --ecr-account 202164733310 \
  --image-tag 1.0.0 \
  --region ap-south-1
```

**What happens:**
1. Creates `mlops` namespace
2. Deploys MLflow (256MB RAM)
3. Deploys API Service (256MB RAM)
4. Deploys Model v2 Service (512MB RAM)
5. Deploys Router with NodePort (128MB RAM)
6. Deploys Prometheus (256MB RAM) - optional
7. Waits for all pods to be ready

**Expected output:**
```
✓ All deployments are ready

NAME                READY   STATUS    AGE
mlflow-xxx          1/1     Running   2m
mlops-api-xxx       1/1     Running   2m
mlops-model-v2-xxx  1/1     Running   2m
mlops-router-xxx    1/1     Running   2m
prometheus-xxx      1/1     Running   2m
```

**Cost checkpoint:** ✅ Services deployed (no additional cost)

---

### Step 4: Verify Deployment (2 minutes)

```bash
# Check all pods are running
kubectl get pods -n mlops

# Check services
kubectl get services -n mlops

# Check resource usage
kubectl top nodes
kubectl top pods -n mlops
```

**Expected resource usage:**
```
NODE                         CPU    MEMORY
ip-10-0-1-xxx.ec2.internal   30%    70%    (1400Mi used, 600Mi free)

POD                          CPU    MEMORY
mlflow-xxx                   5m     150Mi
mlops-api-xxx                10m    180Mi
mlops-model-v2-xxx           15m    450Mi
mlops-router-xxx             5m     80Mi
prometheus-xxx               10m    200Mi
```

---

### Step 5: Test Services (5 minutes)

#### Test via NodePort (Public Access)

```bash
# Use worker node IP from Step 2
WORKER_IP="<your-worker-ip>"

# Test health endpoint
curl http://$WORKER_IP:30080/health

# Expected: {"status":"healthy","service":"router"}

# Test recommendation API
curl -X POST http://$WORKER_IP:30080/recommend \
  -H 'Content-Type: application/json' \
  -d '{"user_id": 1, "top_n": 5}'

# Expected: List of movie recommendations
```

#### Test via Port-Forward (Local Access)

```bash
# In terminal 1: Forward router
kubectl port-forward svc/mlops-router 8080:80 -n mlops

# In terminal 2: Test
curl http://localhost:8080/health
curl -X POST http://localhost:8080/recommend \
  -H 'Content-Type: application/json' \
  -d '{"user_id": 1, "top_n": 5}'
```

#### Access MLflow UI

```bash
# Forward MLflow
kubectl port-forward svc/mlflow 5000:5000 -n mlops

# Open in browser
open http://localhost:5000

# You should see MLflow UI with experiments
```

#### Access Prometheus (if deployed)

```bash
# Forward Prometheus
kubectl port-forward svc/prometheus 9090:9090 -n mlops

# Open in browser
open http://localhost:9090

# Check targets: Status → Targets
```

---

## 📊 What You've Deployed

### Architecture:

```
┌─────────────────────────────────────────┐
│ EKS Control Plane ($73/month)          │
│ - Managed by AWS                        │
│ - High availability                     │
└─────────────────────────────────────────┘
                   │
                   │ kubectl
                   ▼
┌─────────────────────────────────────────┐
│ Worker Node: t3a.small ($7.50/month)   │
│ Public IP: 13.235.xxx.xxx              │
├─────────────────────────────────────────┤
│ Pods:                                   │
│  • MLflow (256MB)                      │
│  • API Service (256MB)                 │
│  • Model v2 Service (512MB)            │
│  • Router (128MB) - NodePort 30080     │
│  • Prometheus (256MB)                  │
│                                         │
│ Total: ~1.4GB / 2GB RAM used          │
└─────────────────────────────────────────┘
                   │
                   │ S3 Integration
                   ▼
┌─────────────────────────────────────────┐
│ S3: mlops-movielens-data ($0.01/month) │
│ ECR: 4 repositories ($0.30/month)      │
└─────────────────────────────────────────┘

Access: http://<worker-ip>:30080
```

### Services Running:

| Service | Purpose | Port | RAM | CPU |
|---------|---------|------|-----|-----|
| MLflow | Experiment tracking | 5000 | 256Mi | 100m |
| API | FastAPI service | 8000 | 256Mi | 200m |
| Model v2 | Flask inference | 8080 | 512Mi | 200m |
| Router | Traffic routing | 30080 | 128Mi | 100m |
| Prometheus | Metrics collection | 9090 | 256Mi | 100m |

### Access Methods:

1. **NodePort:** `http://<worker-ip>:30080` (public)
2. **Port-forward:** `kubectl port-forward` (local)
3. **Internal:** Service DNS within cluster

---

## 💰 Cost Breakdown

### Monthly Costs:

| Item | Cost |
|------|------|
| ✅ EKS Control Plane | $73.00 |
| ✅ EC2 t3a.small × 1 | $7.50 |
| ✅ EBS 30GB gp3 | $2.40 |
| ✅ Data Transfer | $2.00 |
| ✅ S3 Storage | $0.01 |
| ✅ ECR Storage | $0.30 |
| ✅ EBS Snapshots | $0.30 |
| **TOTAL** | **$85.51** |

### Cost Savings Applied:

| Optimization | Savings |
|--------------|---------|
| ✅ No NAT Gateway | $32.85 |
| ✅ No Load Balancer | $16.20 |
| ✅ Single node (not 2) | $28.92 |
| ✅ No CloudWatch logs | $5.00 |
| ✅ No PVCs | $3.00 |
| **Total Saved** | **$85.97/month** |

**Original full EKS:** $177.01/month  
**Minimal EKS:** $85.51/month  
**Savings:** 52% ($91.50/month)

---

## 🎯 Next Steps

### 1. Monitor Your Cluster

```bash
# Check resource usage
watch kubectl top nodes
watch kubectl top pods -n mlops

# View logs
kubectl logs -f deployment/mlops-api -n mlops
kubectl logs -f deployment/mlops-model-v2 -n mlops

# Check events
kubectl get events -n mlops --sort-by='.lastTimestamp'
```

### 2. Test ML Pipeline

```bash
# 1. Check MLflow experiments
open http://localhost:5000  # After port-forward

# 2. Test recommendations for different users
for i in {1..5}; do
  curl -X POST http://$WORKER_IP:30080/recommend \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\": $i, \"top_n\": 5}"
  echo ""
done

# 3. Check metrics
open http://localhost:9090  # Prometheus (if deployed)
```

### 3. Scale Services (if needed)

```bash
# Scale API service to 2 replicas
kubectl scale deployment/mlops-api --replicas=2 -n mlops

# Check status
kubectl get pods -n mlops

# Scale back to 1
kubectl scale deployment/mlops-api --replicas=1 -n mlops
```

### 4. Update Services

```bash
# Build and push new images
cd /Users/tohids/Downloads/MLOps-POC-1-main.
./scripts/aws/build_and_push_to_ecr.sh \
  --region ap-south-1 \
  --version 1.0.1 \
  --push

# Update deployment
kubectl set image deployment/mlops-api \
  api=202164733310.dkr.ecr.ap-south-1.amazonaws.com/mlops-api:1.0.1 \
  -n mlops

# Check rollout status
kubectl rollout status deployment/mlops-api -n mlops
```

---

## 💡 Cost Management

### Option 1: Scale Down When Not in Use

```bash
# Scale node to 0 (EKS control plane still charged $73/month)
eksctl scale nodegroup \
  --cluster mlops-poc-cluster \
  --name mlops-workers \
  --nodes 0

# Costs when scaled to 0:
# - EKS Control Plane: $73/month
# - EC2: $0/month (not running)
# - EBS: $2.40/month (volume persists)
# Total: $75.40/month

# Scale back up when needed
eksctl scale nodegroup \
  --cluster mlops-poc-cluster \
  --name mlops-workers \
  --nodes 1
```

**Savings if used 8 hrs/day:** ~$5/month

### Option 2: Delete Cluster Completely

```bash
# Delete cluster (keeps S3 and ECR)
eksctl delete cluster \
  --name mlops-poc-cluster \
  --region ap-south-1 \
  --wait

# Costs after deletion:
# - S3: $0.01/month
# - ECR: $0.30/month
# Total: $0.31/month

# Recreate cluster when needed (15-20 minutes)
./scripts/aws/create_minimal_eks_cluster.sh
```

**Savings:** $85.20/month when deleted  
**Best for:** Infrequent demos (create/delete as needed)

### Option 3: Use Spot Instances

```bash
# Spot instance is 70% cheaper
# Already created if used --spot flag

# Cost with Spot:
# - EC2 Spot: $2.25/month (vs $7.50)
# - Total: $80.26/month
# - Savings: $5.25/month

# Risk: Can be interrupted with 2-min notice
# Availability: ~90% in ap-south-1
```

---

## 🔍 Troubleshooting

### Pods Not Starting

```bash
# Check pod status
kubectl get pods -n mlops

# Check pod events
kubectl describe pod <pod-name> -n mlops

# Check logs
kubectl logs <pod-name> -n mlops

# Common issues:
# - Image pull errors: Check ECR login
# - OOM: Increase memory limits or reduce replicas
# - Crash loop: Check application logs
```

### Can't Access NodePort

```bash
# Check worker node IP
kubectl get nodes -o wide

# Check security group allows port 30080
aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=*eksctl-mlops-poc*" \
  --region ap-south-1

# Test from local machine
curl -v http://$WORKER_IP:30080/health
```

### Out of Memory

```bash
# Check resource usage
kubectl top nodes
kubectl top pods -n mlops

# Solution 1: Remove Prometheus
kubectl delete deployment/prometheus -n mlops

# Solution 2: Reduce replicas
kubectl scale deployment/mlops-api --replicas=0 -n mlops

# Solution 3: Increase node size (costs more)
# Recreate cluster with t3a.medium instead
```

### Cluster Creation Fails

```bash
# Check eksctl version (need 0.150.0+)
eksctl version

# Check AWS credentials
aws sts get-caller-identity

# Check service quotas
aws service-quotas list-service-quotas \
  --service-code eks \
  --region ap-south-1

# Check CloudFormation stacks
aws cloudformation list-stacks \
  --region ap-south-1 \
  --query "StackSummaries[?contains(StackName, 'eksctl-mlops')]"
```

---

## 🆘 Common Commands

### Cluster Management

```bash
# Get cluster info
kubectl cluster-info
kubectl get nodes
kubectl get pods --all-namespaces

# Update kubeconfig
aws eks update-kubeconfig \
  --name mlops-poc-cluster \
  --region ap-south-1

# Delete cluster
eksctl delete cluster \
  --name mlops-poc-cluster \
  --region ap-south-1
```

### Service Management

```bash
# Get all resources
kubectl get all -n mlops

# Restart deployment
kubectl rollout restart deployment/<name> -n mlops

# Delete service
kubectl delete deployment/<name> -n mlops

# Scale service
kubectl scale deployment/<name> --replicas=N -n mlops
```

### Debugging

```bash
# View logs
kubectl logs -f <pod> -n mlops
kubectl logs <pod> -n mlops --previous

# Shell into pod
kubectl exec -it <pod> -n mlops -- /bin/bash

# Port forward
kubectl port-forward svc/<service> <local>:<remote> -n mlops

# Describe resource
kubectl describe <resource> <name> -n mlops
```

---

## ✅ Success Checklist

After completing all steps, you should have:

- [x] EKS cluster running (1 node)
- [x] All 5 services deployed and healthy
- [x] NodePort accessible at port 30080
- [x] Health checks passing
- [x] Recommendation API working
- [x] MLflow UI accessible
- [x] Prometheus collecting metrics (if deployed)
- [x] Resource usage < 80% on node
- [x] Monthly cost: $85.51 (or $80.26 with Spot)

---

## 📚 Additional Resources

- **Full Cost Analysis:** [docs/MINIMAL_EKS_COST_PLAN.md](../docs/MINIMAL_EKS_COST_PLAN.md)
- **K8s Manifests README:** [k8s-minimal/README.md](../k8s-minimal/README.md)
- **Alternative Solutions:** [docs/COST_EFFECTIVE_MLOPS_ALTERNATIVES.md](../docs/COST_EFFECTIVE_MLOPS_ALTERNATIVES.md)
- **S3 Integration:** [docs/S3_INTEGRATION.md](../docs/S3_INTEGRATION.md)

---

## 🎉 You're Done!

Your minimal EKS cluster is now running with all MLOps services.

**Access your services at:** `http://<worker-ip>:30080`

**Monthly cost:** $85.51 (52% cheaper than full EKS)

**Remember to delete the cluster when done to avoid ongoing charges!**

```bash
eksctl delete cluster --name mlops-poc-cluster --region ap-south-1
```

Happy MLOps! 🚀
