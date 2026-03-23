# EKS Node Group IAM Permission Issue - Analysis & Solutions

## Problem Summary

The EKS cluster control plane was created successfully, but the managed node group is failing due to IAM policy restrictions.

### Error Details
```
Resource handler returned message: "You are not authorized to launch instances 
with this launch template."
```

### Root Cause

Your IAM user has a **restricted_ec2** policy with these conditions:

1. **ALLOW** `ec2:RunInstances` IF:
   - Region = `ap-south-1` AND
   - VPC = `vpc-0a8b35d6ba9e003d1` AND  
   - Instance Type in [t3.nano, t3.micro, t3.small, t3.medium, t3.large, t3.xlarge, m5.large, t3a.xlarge]

2. **DENY** `ec2:RunInstances` IF:
   - VPC ≠ `vpc-0a8b35d6ba9e003d1` OR
   - Instance Type not in allowed list

### Why It's Failing

When EKS creates a managed node group, it uses a **Launch Template** created by the EKS service. The IAM policy evaluation happens when:

- **Principal**: Your IAM user (Vikas.Tiwari@talentica.com)
- **Action**: ec2:RunInstances via EKS Launch Template
- **Resource**: EC2 instances in the specified subnets

The problem is that the **VPC context** in the IAM condition (`ec2:Vpc`) may not match correctly when instances are launched via EKS Launch Templates, even though:
- ✓ We're using the correct VPC: `vpc-0a8b35d6ba9e003d1`
- ✓ We're using allowed instance type: `t3.small`
- ✓ Subnets belong to the correct VPC

## Current Cluster Status

### What Exists:
- ✅ EKS Control Plane: `mlops-poc-cluster` (active)
- ✅ VPC: `vpc-0a8b35d6ba9e003d1`
- ✅ Subnets: 5 subnets across 3 AZs (ap-south-1a, 1b, 1c)
- ✅ Add-ons: vpc-cni, coredns, kube-proxy
- ✅ IAM OIDC Provider

### What's Missing:
- ❌ Worker Nodes (managed node group keeps failing)

###Monthly Cost Currently:
- **$73.00/month** - Just the EKS control plane (no worker nodes)

## Solutions

### Solution 1: Request IAM Policy Update (RECOMMENDED)

**Action**: Contact your AWS administrator to update the `restricted_ec2` policy.

**Option A - Add EKS Service Exception** (Best):
```json
{
  "Sid": "AllowEKSManagedNodeGroups",
  "Effect": "Allow",
  "Action": "ec2:RunInstances",
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "aws:RequestedRegion": "ap-south-1",
      "ec2:Vpc": "arn:aws:ec2:ap-south-1:202164733310:vpc/vpc-0a8b35d6ba9e003d1"
    },
    "StringLike": {
      "aws:userid": "*:eks-*"  // Allow EKS service
    }
  }
} 
```

**Option B - Add Tag-Based Exception**:
```json
{
  "Sid": "AllowEKSTaggedInstances",
  "Effect": "Allow",
  "Action": "ec2:RunInstances",
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "aws:RequestedRegion": "ap-south-1",
      "aws:RequestTag/kubernetes.io/cluster/mlops-poc-cluster": "owned"
    }
  }
}
```

**Pros:**
- Permanent fix
- Allows managed node groups
- Maintains security boundaries

**Cons:**
- Requires admin approval
- May take days to implement

---

### Solution 2: Use EC2 Instances Directly (WORKAROUND)

Since EKS managed node groups aren't working, create EC2 instances directly and join them to the cluster manually.

**Status**: This will likely work because you're launching directly, not via EKS Launch Template.

**Script**: Already created at `scripts/aws/create_self_managed_nodes.sh`

**Steps**:
1. Launch t3.small EC2 in the approved VPC
2. Install kubelet, kubectl, containerd
3. Join to EKS cluster using bootstrap script
4. No managed node group - you manage the instances

**Monthly Cost**: $85.31 (same as managed)

**Pros:**
- Works with current IAM policy
- Full control over instances
- Can start immediately

**Cons:**
- Manual management (no auto-scaling)
- You handle OS updates, patches
- More operational overhead

---

### Solution 3: Use Fargate Instead (ALTERNATIVE)

Use EKS Fargate (serverless) instead of EC2 worker nodes.

**Command**:
```bash
eksctl create fargateprofile \
  --cluster mlops-poc-cluster \
  --name mlops-profile \
  --namespace mlops \
  --region ap-south-1
```

**Monthly Cost**: ~$120/month (more expensive but simpler)
- $73 EKS Control Plane
- $47 Fargate compute (0.5 vCPU, 1GB RAM, 730 hrs)

**Pros:**
- No EC2 IAM issues
- Serverless (AWS manages nodes)
- Auto-scaling built-in

**Cons:**
- More expensive
- Limited to specific pods
- Not all workloads compatible

---

### Solution 4: Use Different AWS Account/Role

If available, use an AWS account or IAM role without these restrictions.

**Options**:
- Assume a different IAM role with broader permissions
- Use a service account with EKS permissions
- Switch to a development/sandbox AWS account

---

## Immediate Next Steps

### Option 1: Wait for IAM Policy Fix
**Timeline**: Unknown (depends on admin)
**Action**: Email AWS admin with Solution 1 details
**Current Cost**: $73/month (control plane only)

### Option 2: Deploy Self-Managed Nodes Now
**Timeline**: 30 minutes
**Action**: Run `./scripts/aws/create_self_managed_nodes.sh`
**Current Cost**: $85.31/month (control plane + 1 node)

### Option 3: Try Fargate
**Timeline**: 15 minutes
**Action**: Run fargate profile creation command above
**Current Cost**: $120/month (control plane + fargate)

### Option 4: Delete Everything and Use Docker Compose
**Timeline**: 10 minutes
**Action**: Run locally with docker-compose (already configured)
**Current Cost**: $0.31/month (just S3 + ECR)

---

## Recommended Path Forward

**SHORT TERM** (Today):
1. Delete the EKS cluster to stop $73/month charge:
   ```bash
   eksctl delete cluster --name mlops-poc-cluster --region ap-south-1
   ```

2. Use Docker Compose locally for development:
   ```bash
   docker-compose up
   ```
   **Cost**: $0.31/month (S3 + ECR only)

**LONG TERM** (After IAM Policy Fixed):
1. Get IAM policy updated (Solution 1)
2. Recreate EKS cluster with working permissions
3. Deploy to production

**ALTERNATIVE** (If IAM Not Fixable):
1. Use self-managed EC2 nodes (Solution 2)
2. Or migrate to different AWS account

---

## Files Created

1. **fix_nodegroup.sh** - Attempts to fix by recreating nodegroup
2. **create_self_managed_nodes.sh** - Manual EC2 node creation (TODO)
3. **IAM_PERMISSION_ANALYSIS.md** - This document

## Summary

The IAM policy is preventing EKS from launching managed nodes even though we're using the correct VPC and instance types. This is because the VPC condition doesn't match properly in the Launch Template context.

**You need to either:**
1. Get the IAM policy updated (best long-term solution)
2. Use self-managed nodes or Fargate (workaround)
3. Use Docker Compose locally until permissions are fixed (cheapest)

The cluster control plane exists and is costing $73/month. Decide whether to keep it (waiting for fix) or delete it (and recreate later).
