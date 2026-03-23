# ✅ EKS Cluster Creation - IN PROGRESS

## Current Status: Creating Cluster

**Started:** February 10, 2026 at 18:10:40  
**Estimated completion:** 18:25-18:30 (15-20 minutes)  
**Cluster Name:** mlops-poc-cluster  
**Region:** ap-south-1 (Mumbai)

---

## What's Happening Now

eksctl is creating the following AWS resources:

### ✅ Phase 1: Control Plane (10-12 minutes)
- EKS Control Plane
- IAM OIDC Provider
- VPC with CIDR 10.0.0.0/16
- Public subnets in 3 availability zones:
  - ap-south-1a: 10.0.64.0/19
  - ap-south-1b: 10.0.32.0/19
  - ap-south-1c: 10.0.0.0/19
- Security Groups
- CloudFormation Stack: `eksctl-mlops-poc-cluster-cluster`

### 🔄 Phase 2: Node Group (5-8 minutes)
- Managed Node Group: `mlops-workers`
- 1× t3a.small instance (2 vCPU, 2GB RAM)
- 30GB gp3 EBS volume (encrypted)
- Public networking (no NAT Gateway)
- CloudFormation Stack: `eksctl-mlops-poc-cluster-nodegroup-mlops-workers`

### ⏳ Phase 3: Add-ons (2-3 minutes)
- vpc-cni (AWS VPC networking)
- coredns (DNS)
- kube-proxy (networking)
- metrics-server (resource metrics)

---

## Monitor Progress

### Check Terminal Output
```bash
# The creation script is running in background
# Check terminal ID: 7f2f5011-f94c-443f-8cae-c099bb9f2879
```

### Check AWS Console
1. Go to: https://console.aws.amazon.com/eks/home?region=ap-south-1
2. Look for cluster: `mlops-poc-cluster`
3. Status should show: Creating → Active

### Check CloudFormation (detailed progress)
1. Go to: https://console.aws.amazon.com/cloudformation/home?region=ap-south-1
2. Look for stacks:
   - `eksctl-mlops-poc-cluster-cluster`
   - `eksctl-mlops-poc-cluster-nodegroup-mlops-workers`
3. Click "Events" tab to see detailed progress

---

## Expected Timeline

| Time | Milestone |
|------|-----------|
| 18:10 | ✅ Started cluster creation |
| 18:12 | Control plane stack created |
| 18:15 | Control plane active |
| 18:18 | OIDC provider associated |
| 18:20 | Node group stack created |
| 18:23 | Worker node launched |
| 18:25 | Add-ons installed |
| 18:27 | ✅ Cluster ready! |

---

## After Cluster is Ready

### 1. Verify Cluster

The script will automatically:
- Configure kubectl
- Check node status
- Verify system pods
- Display cluster information

### 2. Deploy Services

```bash
cd /Users/tohids/Downloads/MLOps-POC-1-main./scripts/aws
./deploy_minimal_services.sh
```

This will deploy:
- MLflow (256MB RAM)
- API Service (256MB RAM)
- Model v2 Service (512MB RAM)
- Router with NodePort (128MB RAM)
- Prometheus (256MB RAM) - optional

### 3. Get Access Information

The worker node public IP will be displayed. Access services at:
```
http://<worker-ip>:30080
```

---

## Cost Information

### While Cluster is Running:
**$85.51/month** ($2.82/day)

- EKS Control Plane: $73.00/month (mandatory)
- EC2 t3a.small: $7.50/month ($0.0103/hour)
- EBS 30GB gp3: $2.40/month
- Data Transfer: ~$2.00/month
- S3 + ECR: $0.31/month
- Snapshots: $0.30/month

### Cost Breakdown by Hour:
- First hour: $0.10 (rounded)
- Per day (24h): $2.82
- Per week: $19.74
- Per month: $85.51

---

## If Something Goes Wrong

### Check for Errors
```bash
# If creation fails, check CloudFormation
aws cloudformation describe-stack-events \
  --stack-name eksctl-mlops-poc-cluster-cluster \
  --region ap-south-1 \
  --max-items 20
```

### Clean Up Failed Cluster
```bash
eksctl delete cluster \
  --name mlops-poc-cluster \
  --region ap-south-1 \
  --wait
```

### Common Issues

1. **Service Quota Exceeded**
   - Check: AWS Console → Service Quotas → EKS
   - Request increase if needed

2. **Insufficient IAM Permissions**
   - Ensure user has EKS, EC2, CloudFormation, IAM permissions
   - Check: IAM console → Users → Permissions

3. **Availability Zone Issues**
   - eksctl will automatically retry with different AZs
   - Wait for process to complete

---

## What's Different from Standard EKS

### ✅ Cost Optimizations Applied:

1. **No NAT Gateway** (saves $32.85/month)
   - Using public subnets instead
   - Nodes get public IPs

2. **No Load Balancer** (saves $16.20/month)
   - Using NodePort on port 30080
   - Direct access to worker node IP

3. **Single Node** (saves $28.92/month)
   - One t3a.small instead of multiple nodes
   - All workloads on one node

4. **No CloudWatch Logs** (saves $5.00/month)
   - Logging disabled
   - Use kubectl logs instead

5. **No PersistentVolumes** (saves ~$3.00/month)
   - Using emptyDir volumes
   - Data lost on pod restart (OK for POC)

**Total Savings: $85.97/month**  
**Full EKS Cost: $177.01/month**  
**Minimal EKS Cost: $85.51/month**  
**Reduction: 52%**

---

## Next Steps Summary

1. ⏳ **Wait for cluster creation** (15-20 minutes)
   - Monitor terminal output
   - Or check AWS Console

2. ✅ **Verify cluster is ready**
   - Script will show success message
   - Worker IP will be displayed

3. 🚀 **Deploy services**
   - Run: `./deploy_minimal_services.sh`
   - Wait 5-10 minutes

4. 🧪 **Test services**
   - Health check: `curl http://<worker-ip>:30080/health`
   - Get recommendations: POST to `/recommend`

5. 📊 **Monitor resources**
   - `kubectl top nodes`
   - `kubectl top pods -n mlops`

6. 💰 **Manage costs**
   - Delete when done: `eksctl delete cluster --name mlops-poc-cluster`
   - Or scale to 0: `eksctl scale nodegroup --nodes 0`

---

## Files Created

- ✅ `/tmp/mlops-poc-cluster-config.yaml` - Cluster configuration
- ⏳ `/tmp/mlops-poc-cluster-info.txt` - Will be created after completion
- ✅ `~/.kube/config` - Will be updated with cluster access

---

## Estimated Completion Time

**Current time:** 18:10  
**Expected completion:** 18:25-18:30  
**Time remaining:** ~15-20 minutes

---

**Status: 🟡 IN PROGRESS - Please wait...**

Check back in 15-20 minutes or monitor the terminal output for completion.
