# EKS Deployment Quick Reference

## Test Deployment Without Full CI/CD

To test EKS deployment without running the entire CI/CD pipeline:

```bash
# Test with latest images
./scripts/test_eks_deployment.sh latest

# Test with specific git commit
./scripts/test_eks_deployment.sh $(git rev-parse HEAD)

# Test with custom tag
./scripts/test_eks_deployment.sh v1.2.3
```

## Manual Deployment Steps

### 1. Verify Cluster Access
```bash
aws eks update-kubeconfig --name mlops-poc-cluster --region ap-south-1
kubectl cluster-info
kubectl get nodes
```

### 2. Check Current Deployments
```bash
kubectl get all -n mlops
kubectl get pods -n mlops -o wide
```

### 3. Deploy/Update Services
```bash
# Apply manifests
kubectl apply -f k8s-minimal/02-api-service.yaml -n mlops
kubectl apply -f k8s-minimal/03-model-v2-service.yaml -n mlops
kubectl apply -f k8s-minimal/04-router-service.yaml -n mlops

# Update images (use correct container names!)
IMAGE_TAG="latest"  # or your git SHA
ECR_REGISTRY="202164733310.dkr.ecr.ap-south-1.amazonaws.com"

kubectl set image deployment/mlops-api api=$ECR_REGISTRY/mlops-api:$IMAGE_TAG -n mlops
kubectl set image deployment/mlops-model-v2 model-v2=$ECR_REGISTRY/mlops-model-v2:$IMAGE_TAG -n mlops
kubectl set image deployment/mlops-router router=$ECR_REGISTRY/mlops-router:$IMAGE_TAG -n mlops
```

### 4. Monitor Rollout
```bash
# Watch rollout status
kubectl rollout status deployment/mlops-api -n mlops
kubectl rollout status deployment/mlops-model-v2 -n mlops
kubectl rollout status deployment/mlops-router -n mlops

# Check pod status
kubectl get pods -n mlops -w
```

### 5. Access Services
```bash
# Get external IP and NodePort
EXTERNAL_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
NODEPORT=$(kubectl get svc mlops-router -n mlops -o jsonpath='{.spec.ports[0].nodePort}')

# Test the service
curl http://$EXTERNAL_IP:$NODEPORT/health
```

## Container Names (IMPORTANT!)

The container names in deployments are:
- **mlops-api** deployment → container name: `api`
- **mlops-model-v2** deployment → container name: `model-v2`
- **mlops-router** deployment → container name: `router`

❌ **Wrong:**
```bash
kubectl set image deployment/mlops-api mlops-api=...
```

✅ **Correct:**
```bash
kubectl set image deployment/mlops-api api=...
```

## Troubleshooting

### Check Pod Logs
```bash
kubectl logs -n mlops deployment/mlops-api
kubectl logs -n mlops deployment/mlops-model-v2
kubectl logs -n mlops deployment/mlops-router
```

### Describe Deployment
```bash
kubectl describe deployment mlops-api -n mlops
kubectl describe pod -n mlops -l app=mlops-api
```

### Rollback Deployment
```bash
kubectl rollout undo deployment/mlops-api -n mlops
kubectl rollout undo deployment/mlops-model-v2 -n mlops
kubectl rollout undo deployment/mlops-router -n mlops
```

### Check Image Version
```bash
kubectl get deployment mlops-api -n mlops -o jsonpath='{.spec.template.spec.containers[0].image}'
```

## Common Issues

### Issue: "unable to find container named mlops-api"
**Cause:** Using deployment name instead of container name
**Fix:** Use `api`, `model-v2`, `router` as container names

### Issue: "--record has been deprecated"
**Cause:** Using deprecated --record flag
**Fix:** Remove --record flag from kubectl commands (already fixed in workflow)

### Issue: Images not updating
**Cause:** Image pull policy or wrong tag
**Fix:** 
```bash
# Force restart pods
kubectl rollout restart deployment/mlops-api -n mlops
```

## Quick Health Check

```bash
#!/bin/bash
# Quick health check script
for deploy in mlops-api mlops-model-v2 mlops-router; do
  echo -n "$deploy: "
  kubectl get deployment $deploy -n mlops -o jsonpath='{.status.availableReplicas}/{.spec.replicas}'
  echo ""
done
```

## Current Cluster Configuration

- **Cluster Name:** mlops-poc-cluster
- **Region:** ap-south-1
- **Nodes:** 2 x t3.small (2 vCPU, 2GB RAM each)
- **External IPs:** 13.127.21.160, 13.201.173.4
- **Router NodePort:** 31233

**Access URL:** http://13.127.21.160:31233/
