# EKS Cluster Ready - Complete Configuration Checklist

## ✅ Cluster Successfully Created!

**Cluster Name:** mlops-poc-cluster  
**Region:** ap-south-1 (Mumbai)  
**Status:** ACTIVE  
**Created:** February 10, 2026

---

## 🎯 Infrastructure Components

### ✅ EKS Control Plane
- **Version:** 1.31
- **Endpoint:** https://1B67B6CCBB0AC9D6268E58FC83D30990.gr7.ap-south-1.eks.amazonaws.com
- **Status:** Active and Healthy
- **Public Access:** Enabled
- **Private Access:** Enabled

### ✅ Worker Nodes
- **Node Count:** 1 (Ready)
- **Instance Type:** t3.small
  - 2 vCPU
  - 2 GB RAM
  - 30 GB gp3 storage (encrypted)
- **Node Name:** ip-10-0-3-82.ap-south-1.compute.internal
- **Internal IP:** 10.0.3.82
- **External IP:** 13.233.40.215
- **OS:** Amazon Linux 2023
- **Container Runtime:** containerd 2.1.5

### ✅ Networking
- **VPC:** vpc-0a8b35d6ba9e003d1 (radstrongcoaching)
- **Subnets:**
  - subnet-07dc8805394df201d (ap-south-1c, 10.0.4.0/24) - Public IP enabled
  - subnet-0d34bb751773af6da (ap-south-1a, 10.0.3.0/24) - Public IP enabled
- **Security Groups:**
  - Cluster SG: sg-0ce3d100378437429
  - Control Plane SG: sg-0591ca63d3dfd844f

### ✅ System Add-ons
- **vpc-cni:** v1.20.4-eksbuild.2 (ACTIVE)
- **coredns:** v1.11.3-eksbuild.1 (ACTIVE, 2 replicas)
- **kube-proxy:** v1.31.10-eksbuild.12 (ACTIVE)

### ✅ IAM & Security
- **OIDC Provider:** Enabled
- **OIDC Issuer:** https://oidc.eks.ap-south-1.amazonaws.com/id/1B67B6CCBB0AC9D6268E58FC83D30990
- **IAM User:** Vikas.Tiwari@talentica.com
- **IAM Policy:** restricted_ec2 (v6 - Fixed for EKS Launch Templates)

### ✅ Storage
- **Storage Class:** gp2 (kubernetes.io/aws-ebs)
- **Default:** Yes
- **Reclaim Policy:** Delete
- **Volume Binding:** WaitForFirstConsumer

### ✅ Kubectl Configuration
- **Context:** arn:aws:eks:ap-south-1:202164733310:cluster/mlops-poc-cluster
- **Kubeconfig:** ~/.kube/config
- **API Connectivity:** ✅ Healthy

---

## 📦 Ready Components

### Namespaces
- ✅ default
- ✅ kube-system
- ✅ kube-node-lease
- ✅ kube-public

### System Pods (4 running)
- ✅ aws-node-z2kz6 (VPC CNI)
- ✅ coredns-5f7c9b478-5vgp4
- ✅ coredns-5f7c9b478-w2dd4
- ✅ kube-proxy-x9lk5

---

## 💰 Cost Breakdown

| Component | Monthly Cost |
|-----------|--------------|
| EKS Control Plane | $73.00 |
| EC2 t3.small (1 node) | $14.60 |
| EBS gp3 30GB | $2.40 |
| Data Transfer (est.) | $1.00 |
| **Total** | **~$91.00/month** |

---

## 🚀 Next Steps: Deploy MLOps Services

### Option 1: Automated Deployment (Recommended)
```bash
cd /Users/tohids/Downloads/MLOps-POC-1-main./scripts/aws
./deploy_minimal_services.sh
```

This will deploy:
- MLflow tracking server
- FastAPI service
- Model v2 inference service
- Router service (NodePort)
- Prometheus monitoring (optional)

### Option 2: Manual Deployment
```bash
cd /Users/tohids/Downloads/MLOps-POC-1-main.
kubectl create namespace mlops
kubectl apply -f k8s-minimal/01-mlflow.yaml
kubectl apply -f k8s-minimal/02-api-service.yaml
kubectl apply -f k8s-minimal/03-model-v2-service.yaml
kubectl apply -f k8s-minimal/04-router-service.yaml
kubectl apply -f k8s-minimal/05-prometheus-minimal.yaml  # Optional
```

### Verify Deployment
```bash
# Watch pods being created
kubectl get pods -n mlops -w

# Check services
kubectl get svc -n mlops

# Get NodePort URL
echo "Router URL: http://$(kubectl get nodes -o wide --no-headers | awk '{print $7}'):$(kubectl get svc router -n mlops -o jsonpath='{.spec.ports[0].nodePort}')"
```

---

## 🔍 Monitoring & Management Commands

### Check Cluster Health
```bash
kubectl get nodes
kubectl get pods --all-namespaces
kubectl top nodes  # Requires metrics-server
```

### View Logs
```bash
kubectl logs <pod-name> -n mlops
kubectl logs -f <pod-name> -n mlops  # Follow logs
```

### Access Shell in Pod
```bash
kubectl exec -it <pod-name> -n mlops -- /bin/bash
```

### Scale Deployment (if needed in future)
```bash
eksctl scale nodegroup --cluster=mlops-poc-cluster --name=mlops-workers --nodes=2 --region=ap-south-1
```

---

## 🛠️ Useful Scripts Created

1. **cluster_status.sh** - Full cluster configuration summary
2. **update_iam_policy_final_fix.sh** - IAM policy fixer for EKS
3. **create_eks_final.sh** - Complete cluster creation script
4. **deploy_minimal_services.sh** - Service deployment automation

---

## 📝 IAM Policy Fix Applied

**Problem:** Launch Templates didn't pass `ec2:Vpc` context, causing DENY
**Solution:** Removed VPC DENY statement, added Launch Template-specific ALLOW

**Current Policy Version:** v6
- ✅ Allows EKS to launch instances via Launch Templates
- ✅ Maintains region restriction (ap-south-1 only)
- ✅ Maintains instance type restriction (t3.small allowed)
- ✅ No VPC restriction for Launch Template instances

---

## 🎉 SUCCESS SUMMARY

✅ EKS cluster created and active  
✅ 1 worker node running and ready  
✅ All system components healthy  
✅ Networking configured correctly  
✅ IAM permissions fixed  
✅ Kubectl configured and connected  
✅ Ready for application deployment  

**The cluster is fully operational and ready to host your MLOps workload!**

---

## 📞 Support & Troubleshooting

### View Cluster Events
```bash
kubectl get events --all-namespaces --sort-by='.lastTimestamp'
```

### Check Add-on Status
```bash
aws eks list-addons --cluster-name mlops-poc-cluster --region ap-south-1
aws eks describe-addon --cluster-name mlops-poc-cluster --addon-name vpc-cni --region ap-south-1
```

### Delete Cluster (if needed)
```bash
eksctl delete cluster --name mlops-poc-cluster --region ap-south-1
```

---

**Documentation Generated:** February 10, 2026  
**Cluster ID:** mlops-poc-cluster  
**Region:** ap-south-1
