# Minimal EKS Cluster for MLOps POC - Cost Estimation & Plan

**Date:** February 10, 2026  
**Region:** ap-south-1 (Mumbai)  
**Purpose:** POC/Demo MLOps Lifecycle  

---

## 🎯 Requirements

✅ **What We Need:**
- EKS cluster for Kubernetes orchestration
- Single node (no high availability needed for POC)
- No Network Load Balancer (use port-forward or NodePort)
- No NAT Gateway (use public subnet)
- MLOps services: MLflow, API, Model serving, Monitoring

❌ **What We DON'T Need for POC:**
- Multiple node groups
- Auto-scaling (manual scaling OK)
- Load Balancer (ALB/NLB)
- NAT Gateway
- High availability
- Production-grade monitoring

---

## 💰 Cost Breakdown - Minimal EKS for POC

### Monthly Cost: **$85.50**

| Component | Specification | Monthly Cost | Annual Cost | Can Remove? |
|-----------|--------------|--------------|-------------|-------------|
| **EKS Control Plane** | Managed Kubernetes | **$73.00** | $876.00 | ❌ Mandatory |
| **EC2 Worker Node** | t3a.small (1 node) | **$7.50** | $90.00 | ❌ Need compute |
| **EBS Volume** | 30GB gp3 | **$2.40** | $28.80 | ❌ Need storage |
| **Elastic IP** | For worker node | **$0.00** | $0.00 | ✅ Free if attached |
| **Data Transfer** | Minimal (~5GB/month) | **$2.00** | $24.00 | ⚠️ Usage-based |
| **S3 Storage** | Existing (23.75 MB) | **$0.01** | $0.12 | ✅ Already have |
| **ECR Storage** | Existing (~3 GB) | **$0.30** | $3.60 | ✅ Already have |
| **Snapshots** | Weekly backup | **$0.30** | $3.60 | ✅ Optional |
| | | | | |
| **TOTAL** | | **$85.51/month** | **$1,026.12/year** | |

---

### Cost Comparison

| Setup | Monthly Cost | Savings vs Full EKS |
|-------|--------------|---------------------|
| **Full EKS (original)** | $177.01 | Baseline |
| **Minimal EKS (this plan)** | **$85.51** | **52% ($91.50)** |
| **EC2 with Docker Compose** | $10.26 | 94% ($166.75) |
| **Local Development** | $0.36 | 99.8% ($176.65) |

---

## 🏗️ Architecture - Minimal EKS POC

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS Cloud (ap-south-1)                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ EKS Control Plane ($73/month - Mandatory)            │ │
│  │ - Managed Kubernetes API                             │ │
│  │ - etcd storage                                        │ │
│  │ - High availability (AWS managed)                    │ │
│  └───────────────────────────────────────────────────────┘ │
│                           │                                 │
│                           │ kubectl                         │
│                           ▼                                 │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ VPC (Public Subnet - No NAT Gateway)                 │ │
│  │                                                       │ │
│  │  ┌─────────────────────────────────────────────────┐ │ │
│  │  │ Single EC2 Worker Node (t3a.small)             │ │ │
│  │  │ $7.50/month                                     │ │ │
│  │  │ ----------------------------------------        │ │ │
│  │  │ vCPU: 2 | RAM: 2GB | Disk: 30GB gp3          │ │ │
│  │  │ ----------------------------------------        │ │ │
│  │  │                                                 │ │ │
│  │  │ Pods Running:                                   │ │ │
│  │  │  ├─ MLflow (256MB RAM, 100m CPU)              │ │ │
│  │  │  ├─ API Service (256MB RAM, 200m CPU)         │ │ │
│  │  │  ├─ Model v2 (512MB RAM, 200m CPU)            │ │ │
│  │  │  ├─ Router (128MB RAM, 100m CPU)              │ │ │
│  │  │  └─ Prometheus (256MB RAM, 100m CPU)          │ │ │
│  │  │     (Optional: Grafana)                        │ │ │
│  │  │                                                 │ │ │
│  │  │ Total: ~1.4GB RAM, ~700m CPU                  │ │ │
│  │  │ Headroom: ~600MB RAM, 1300m CPU               │ │ │
│  │  └─────────────────────────────────────────────────┘ │ │
│  │            │                                           │ │
│  │            │ Elastic IP (Free)                        │ │
│  │            │ Public Access: NodePort or Port-Forward  │ │
│  └───────────────────────────────────────────────────────┘ │
│                           │                                 │
│  ┌────────────────────────┴──────────────────────────────┐ │
│  │ S3 Bucket (mlops-movielens-data) - $0.01/month      │ │
│  │ - Raw data (23.75 MB)                                 │ │
│  │ - MLflow artifacts                                    │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ ECR (Container Registry) - $0.30/month               │ │
│  │ - mlops-api                                           │ │
│  │ - mlops-model-v2                                      │ │
│  │ - mlops-router                                        │ │
│  │ - mlops-mlflow (optional)                            │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Access via: kubectl port-forward or NodePort
No Load Balancer needed for POC
```

---

## 📋 Detailed Cost Analysis

### 1. EKS Control Plane: $73/month (**Mandatory**)

**What it provides:**
- Managed Kubernetes API server
- etcd cluster (3 nodes for HA)
- Controller manager & scheduler
- AWS integration (IAM, VPC, etc.)

**Why it costs:**
- AWS manages infrastructure
- High availability (3 AZs)
- Automatic upgrades & patching
- 99.95% SLA

**Can't avoid:** This is the base cost of EKS. No workaround.

**Alternatives:**
- Self-managed K8s (K3s, kubeadm): $0 but complex
- Docker Compose: $0 but no K8s
- ECS: $0 control plane, but different orchestrator

---

### 2. EC2 Worker Node: $7.50/month

**Configuration:**
```yaml
Instance Type: t3a.small
Architecture: x86_64 (AMD EPYC)
vCPU: 2 (burstable)
RAM: 2GB
CPU Credits: 24/hour
Network: Up to 5 Gbps
```

**Why t3a.small?**
- Cheapest viable option for K8s node
- 2GB RAM sufficient for small pods
- Burstable CPU for cost efficiency
- AMD EPYC (10% cheaper than Intel t3)

**Resource allocation:**
```
Total RAM: 2048 MB
- Reserved for kubelet/system: ~400MB
- Available for pods: ~1600MB

Total CPU: 2000m
- Reserved for system: ~200m
- Available for pods: ~1800m
```

**Cost options:**
| Pricing Model | Cost/Month | Risk | Recommendation |
|---------------|------------|------|----------------|
| **On-Demand** | $7.50 | None | ✅ Recommended for POC |
| **Spot Instance** | $2.25 | Can be terminated | Good for dev |
| **Reserved (1yr)** | $4.56 | Commitment | If long-term |

---

### 3. EBS Volume: $2.40/month

**Configuration:**
```yaml
Type: gp3 (General Purpose SSD)
Size: 30GB
IOPS: 3000 (baseline)
Throughput: 125 MB/s
```

**Storage breakdown:**
```
/                     5GB   (OS, system files)
/var/lib/kubelet      8GB   (Container images, pods)
/var/log              2GB   (Kubernetes + application logs)
/var/lib/docker       10GB  (Docker layers)
/tmp                  2GB   (Temporary files)
Free space            3GB   (Buffer)
```

**Why 30GB?**
- Minimum viable for K8s node
- Docker images: ~8-10GB
- System + logs: ~5-7GB
- Safety buffer: ~3GB

**Alternatives:**
| Size | Monthly Cost | Trade-off |
|------|--------------|-----------|
| 20GB | $1.60 | Too tight, may run out |
| **30GB** | **$2.40** | **Recommended** |
| 50GB | $4.00 | Safer but overkill for POC |

---

### 4. Networking Costs

#### VPC & Subnets: **$0/month** (Free)
- VPC is free
- Subnets are free
- Internet Gateway is free
- Route tables are free

#### NAT Gateway: **$0/month** (Avoided!)
- Normal cost: $32.85/month
- **Strategy:** Use public subnet for worker node
- Worker node gets public IP directly
- Pods access internet via Internet Gateway

**Configuration:**
```yaml
VPC: 10.0.0.0/16
Public Subnet: 10.0.1.0/24
  - Worker node: 10.0.1.10
  - Public IP: Elastic IP (free when attached)
  - Route: 0.0.0.0/0 → Internet Gateway
```

#### Load Balancer: **$0/month** (Avoided!)
- Normal NLB cost: $16.20/month
- Normal ALB cost: $22.50/month
- **Strategy:** Use NodePort or kubectl port-forward

**Access methods:**
```bash
# Option 1: NodePort (expose on worker node IP)
apiVersion: v1
kind: Service
spec:
  type: NodePort
  ports:
    - port: 80
      nodePort: 30080  # Access via <worker-ip>:30080

# Option 2: Port forwarding (for testing)
kubectl port-forward svc/mlops-router 8080:80 -n mlops

# Option 3: Use Nginx Ingress Controller (free software)
# Still no AWS Load Balancer, just NodePort
```

#### Data Transfer: ~$2/month
- First 1GB out to internet: Free
- Next 9.999TB: $0.09/GB
- Estimate: 20GB/month = $1.80

**Breakdown:**
```
Docker image pulls (ECR):    ~2GB/month  = $0.18
S3 data access:               ~1GB/month  = $0.09
API responses:                ~1GB/month  = $0.09
Monitoring/logs:              ~1GB/month  = $0.09
Total estimate:               ~5GB/month  = $0.45
```

---

### 5. Storage Costs

#### S3: $0.01/month (Already have)
```
Current usage: 23.75 MB
- movies.dat: 167 KB
- ratings.dat: 23 MB
- users.dat: 131 KB
- MLflow artifacts: minimal

Cost: $0.023 per GB-month
23.75 MB = 0.023 GB = $0.0005/month (negligible)
```

#### ECR: $0.30/month (Already have)
```
Current images: ~3GB
- mlops-api: ~800MB
- mlops-model-v2: ~1.2GB
- mlops-router: ~500MB
- mlops-mlflow: 0MB (not used)

Cost: $0.10 per GB-month
3GB × $0.10 = $0.30/month
```

#### EBS Snapshots: $0.30/month (Optional)
```
Strategy: Weekly snapshots of 30GB EBS
Retention: 4 snapshots (1 month)

Cost: $0.05 per GB-month for snapshots
30GB × 0.25 (incremental) × $0.05 = $0.38/month

Recommendation: Manual snapshots before major changes
```

---

## 🔧 Technical Configuration

### EKS Cluster Specification

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: mlops-poc-cluster
  region: ap-south-1
  version: "1.31"

# No CloudWatch logging (saves ~$5/month)
cloudWatch:
  clusterLogging:
    enableTypes: []

# OIDC provider (needed for service accounts)
iam:
  withOIDC: true

# Single node group - minimal configuration
managedNodeGroups:
  - name: mlops-workers
    instanceType: t3a.small
    minSize: 1
    maxSize: 1
    desiredCapacity: 1
    volumeSize: 30
    volumeType: gp3
    
    # Use public subnet (no NAT needed)
    privateNetworking: false
    
    # Resource tagging
    tags:
      Name: mlops-poc-worker
      Environment: POC
      Project: mlops
      CostCenter: mlops-poc
    
    # IAM policies for AWS service access
    iam:
      withAddonPolicies:
        imageBuilder: true    # For ECR
        ebs: true            # For EBS volumes
        cloudWatch: false    # Disable to save cost

# VPC configuration - public subnet only
vpc:
  # No NAT Gateway
  nat:
    gateway: Disable
  
  # Public subnets only
  subnets:
    public:
      ap-south-1a:
        id: "subnet-xxxxx"  # Your subnet ID
```

---

### Pod Resource Limits (Optimized for 2GB RAM)

```yaml
# MLflow - Lightweight
resources:
  requests:
    memory: "128Mi"
    cpu: "50m"
  limits:
    memory: "256Mi"
    cpu: "100m"

# API Service - Moderate
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "256Mi"
    cpu: "200m"

# Model v2 Service - Needs more memory
resources:
  requests:
    memory: "256Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "200m"

# Router - Lightweight
resources:
  requests:
    memory: "64Mi"
    cpu: "50m"
  limits:
    memory: "128Mi"
    cpu: "100m"

# Prometheus - Moderate (optional)
resources:
  requests:
    memory: "128Mi"
    cpu: "50m"
  limits:
    memory: "256Mi"
    cpu: "100m"
```

**Total resource usage:**
```
Memory: 128 + 128 + 256 + 64 + 128 = 704 MB requested
        256 + 256 + 512 + 128 + 256 = 1408 MB limit
        
CPU:    50 + 100 + 100 + 50 + 50 = 350m requested
        100 + 200 + 200 + 100 + 100 = 700m limit

Available on t3a.small:
  Memory: ~1600MB (after system overhead)
  CPU: ~1800m (after system overhead)

✅ Fits comfortably within limits
```

---

## 📊 Service Deployment Strategy

### What to Deploy on EKS

```yaml
✅ Deploy on EKS:
  - MLflow Tracking Server (experiment tracking)
  - API Service (FastAPI)
  - Model v2 Service (Flask inference)
  - Router Service (traffic routing)
  - Prometheus (basic metrics - optional)

❌ Don't Deploy on EKS (to save resources):
  - Grafana (run locally if needed)
  - Training jobs (run on local machine or separate EC2)
  - Data processing (run locally or Lambda)
```

### Access Strategy

**No Load Balancer - Use NodePort:**

```yaml
# Example: Router service with NodePort
apiVersion: v1
kind: Service
metadata:
  name: mlops-router
  namespace: mlops
spec:
  type: NodePort
  ports:
    - port: 80
      targetPort: 8080
      nodePort: 30080  # Fixed port 30080
  selector:
    app: mlops-router
```

**Access:**
```bash
# Get worker node IP
kubectl get nodes -o wide

# Access service
curl http://<WORKER-NODE-IP>:30080/health

# Or use port-forward for testing
kubectl port-forward svc/mlops-router 8080:80 -n mlops
curl http://localhost:8080/health
```

---

## 🚀 Implementation Plan

### Phase 1: Setup (30 minutes)

1. **Create EKS Cluster**
   ```bash
   eksctl create cluster -f cluster-config.yaml
   # Takes 15-20 minutes
   ```

2. **Verify Cluster**
   ```bash
   kubectl get nodes
   kubectl get pods --all-namespaces
   ```

3. **Configure kubectl**
   ```bash
   aws eks update-kubeconfig --name mlops-poc-cluster --region ap-south-1
   ```

---

### Phase 2: Deploy Services (20 minutes)

1. **Create Namespace**
   ```bash
   kubectl create namespace mlops
   ```

2. **Deploy MLflow**
   ```bash
   kubectl apply -f k8s/mlflow-minimal.yaml
   ```

3. **Deploy Application Services**
   ```bash
   kubectl apply -f k8s/api-service-minimal.yaml
   kubectl apply -f k8s/model-v2-service-minimal.yaml
   kubectl apply -f k8s/router-service-minimal.yaml
   ```

4. **Verify Deployments**
   ```bash
   kubectl get pods -n mlops
   kubectl get services -n mlops
   ```

---

### Phase 3: Access & Test (10 minutes)

1. **Get Worker Node IP**
   ```bash
   WORKER_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
   echo $WORKER_IP
   ```

2. **Test Services**
   ```bash
   # Via NodePort
   curl http://$WORKER_IP:30080/health
   
   # Or via port-forward
   kubectl port-forward svc/mlops-router 8080:80 -n mlops
   curl http://localhost:8080/health
   ```

3. **Access MLflow UI**
   ```bash
   kubectl port-forward svc/mlflow 5000:5000 -n mlops
   # Open http://localhost:5000
   ```

---

## 💡 Cost Optimization Strategies

### 1. Use Spot Instance for Worker (Save 70%)

```yaml
# In cluster config:
managedNodeGroups:
  - name: mlops-workers
    instanceType: t3a.small
    instancesDistribution:
      onDemandBaseCapacity: 0
      onDemandPercentageAboveBaseCapacity: 0
      spotAllocationStrategy: capacity-optimized
```

**Savings:** $7.50 → $2.25 = **$5.25/month saved**
**New total:** $85.51 → $80.26/month

**Risk:** Spot can be interrupted (90% availability in ap-south-1)

---

### 2. Stop Cluster When Not in Use

```bash
# Scale node group to 0 when not using
eksctl scale nodegroup --cluster mlops-poc-cluster \
  --name mlops-workers --nodes 0

# Costs when scaled to 0:
# - EKS Control Plane: $73/month (still charged)
# - EC2: $0/month (not running)
# - EBS: $2.40/month (volume still exists)
# Total: $75.40/month

# Scale back up when needed
eksctl scale nodegroup --cluster mlops-poc-cluster \
  --name mlops-workers --nodes 1
```

**Savings if used 8 hours/day:**
- EC2: $7.50 × 33% = $2.50/month
- **New total:** $77.91/month

---

### 3. Use Fargate Instead of EC2 (Alternative)

```yaml
# Fargate pricing: $0.04048/vCPU/hour + $0.004445/GB/hour
# For 0.25 vCPU, 0.5GB per pod:
# Cost per pod per month: ~$15-20

Comparison:
- EC2 t3a.small: $7.50/month (can run 5+ pods)
- Fargate (5 pods): $75-100/month

Verdict: EC2 is cheaper for continuous workloads
```

---

### 4. Delete Cluster When Done Testing

```bash
# Delete entire cluster
eksctl delete cluster --name mlops-poc-cluster --region ap-south-1

# Keeps:
# - S3 data ($0.01/month)
# - ECR images ($0.30/month)
# Total: $0.31/month

# Can recreate cluster anytime in 15 minutes
```

**Best for:** Short-term POC demos

---

## 📅 Cost Projection Scenarios

### Scenario 1: Full-Time POC (24/7 for 1 month)

| Component | Cost |
|-----------|------|
| EKS Control Plane | $73.00 |
| EC2 t3a.small | $7.50 |
| EBS 30GB | $2.40 |
| Data Transfer | $2.00 |
| S3 + ECR | $0.31 |
| Snapshots | $0.30 |
| **Total** | **$85.51** |

---

### Scenario 2: Business Hours Only (8hrs/day, 22 days)

| Component | Cost |
|-----------|------|
| EKS Control Plane | $73.00 |
| EC2 (scaled down 67% of time) | $2.50 |
| EBS 30GB | $2.40 |
| Data Transfer | $1.00 |
| S3 + ECR | $0.31 |
| Snapshots | $0.30 |
| **Total** | **$79.51** |

---

### Scenario 3: Weekly Demos Only (8hrs/week)

| Component | Cost |
|-----------|------|
| EKS Control Plane | $73.00 |
| EC2 (scaled down 95% of time) | $0.38 |
| EBS 30GB | $2.40 |
| Data Transfer | $0.50 |
| S3 + ECR | $0.31 |
| Snapshots | $0.30 |
| **Total** | **$76.89** |

---

### Scenario 4: Create for Demo, Delete After (1 week)

| Component | Weekly Cost |
|-----------|-------------|
| EKS Control Plane | $16.90 |
| EC2 t3a.small | $1.74 |
| EBS 30GB | $0.56 |
| Data Transfer | $0.50 |
| S3 + ECR | $0.31 |
| **Total** | **$20.01/week** |

**Recommendation:** Create cluster only when needed for demos!

---

## ⚖️ EKS vs Alternatives Comparison

| Solution | Monthly Cost | Setup Time | Complexity | K8s Native | Best For |
|----------|--------------|------------|------------|------------|----------|
| **Minimal EKS** | **$85.51** | 30 min | Medium | ✅ Yes | K8s POC/Demo |
| **EKS (Spot)** | **$80.26** | 30 min | Medium | ✅ Yes | Dev/Test |
| **EC2 + Docker Compose** | $10.26 | 30 min | Low | ❌ No | Simple POC |
| **EC2 + K3s** | $10.26 | 60 min | High | ✅ Yes | Self-managed K8s |
| **Local (docker-compose)** | $0.36 | 5 min | Low | ❌ No | Development |
| **ECS** | $10.26 | 30 min | Low | ❌ No | AWS-native |

---

## ✅ Decision Matrix

### Choose Minimal EKS if:
✅ You need to demonstrate Kubernetes features  
✅ You want AWS-managed K8s (less maintenance)  
✅ You need K8s-native features (HPA, deployments, etc.)  
✅ Budget allows $85-90/month  
✅ You value AWS support and SLA  

### Choose EC2 + Docker Compose if:
✅ Budget is very tight (< $15/month)  
✅ You don't need Kubernetes specifically  
✅ Simplicity is more important than K8s  
✅ You're comfortable with Docker basics  
✅ POC is for ML pipeline, not K8s features  

### Choose Local Development if:
✅ Budget is minimal ($0.36/month)  
✅ Just testing ML models and features  
✅ No need for cloud deployment yet  
✅ Can demo from laptop  
✅ Early development phase  

---

## 📋 Final Recommendation

### For Your MLOps POC:

**Option A: Minimal EKS (Recommended if K8s required)**
```
Monthly Cost: $85.51
- Perfect for K8s POC/demo
- AWS-managed, less maintenance
- Can scale up later if needed
- Good for stakeholder presentations
```

**Option B: EC2 + Docker Compose (Recommended if budget-constrained)**
```
Monthly Cost: $10.26
- 88% cheaper than EKS
- All MLOps features work
- Simpler to manage
- Easy to understand
- Can migrate to EKS later
```

**My Suggestion:** Start with **Option B (EC2)** for development, then create **Option A (EKS)** only for demos/presentations. This gives you:
- Development: $10.26/month
- Demo days: +$85.51/month (only when needed)
- Average: ~$20-30/month if demoing 1-2 times

---

## 🛠️ Next Steps

If you choose **Minimal EKS**, I can create:
1. ✅ Cluster configuration YAML
2. ✅ Minimal K8s manifests (optimized for 2GB RAM)
3. ✅ Deployment scripts
4. ✅ Access configuration (NodePort)
5. ✅ Cost monitoring setup

If you choose **EC2 + Docker Compose**, I can create:
1. ✅ EC2 launch configuration
2. ✅ Docker Compose file
3. ✅ Deployment automation
4. ✅ Migration path to EKS later

**Which option would you like to proceed with?**
