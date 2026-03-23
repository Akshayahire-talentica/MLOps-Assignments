# airflow-on-eks-manual-gitops

# 📘 Airflow on EKS with Helm (Manual GitOps)

This project demonstrates how to deploy **Apache Airflow** on an **Amazon EKS** (Elastic Kubernetes Service) cluster using **Helm** and a **manual GitOps approach** (without Argo CD or Terraform). It's a great way to learn DevOps for data-oriented workflows in Kubernetes.

---

## 🚀 Features

- Apache Airflow deployment on EKS
- PostgreSQL as Airflow's metadata DB
- DAGs managed via ConfigMap (manual GitOps)
- Helm-based component installation
- YAML manifests to manage all Kubernetes objects
- Airflow UI exposed via LoadBalancer

---

## 🧰 Tech Stack

- Amazon EKS (via `eksctl`)
- Kubernetes (`kubectl`)
- Helm
- Apache Airflow
- PostgreSQL
- Git (manual GitOps)
- VS Code (dev environment)

---

## 🧱 Directory Structure

airflow-on-eks-manual-gitops/
├── dags/
│ └── example_dag.py
├── manifests/
│ ├── airflow-init-job.yaml
│ ├── airflow-scheduler-deployment.yaml
│ ├── airflow-web-deployment.yaml
│ ├── airflow-web-service.yaml
│ ├── airflow-dags-configmap.yaml
│ ├── configmap.yaml
│ ├── secret.yaml
│ ├── postgres-deployment.yaml
│ └── postgres-service.yaml


---

## 📦 Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [eksctl](https://eksctl.io/)
- [Helm](https://helm.sh/)
- AWS account with IAM permissions to create EKS clusters

---

## ✅ Setup Guide

### 1. Create EKS Cluster

eksctl create cluster --name airflow-cluster --region ap-south-1 --nodes 2

## 2. Deploy PostgreSQL

kubectl apply -f manifests/postgres-deployment.yaml
kubectl apply -f manifests/postgres-service.yaml

## 3. Apply ConfigMap & Secrets

kubectl apply -f manifests/configmap.yaml
kubectl apply -f manifests/secret.yaml

## 4. Run Airflow Init Job

kubectl apply -f manifests/airflow-init-job.yaml

## 5. Deploy Airflow Web + Scheduler

kubectl apply -f manifests/airflow-web-deployment.yaml
kubectl apply -f manifests/airflow-scheduler-deployment.yaml
kubectl apply -f manifests/airflow-web-service.yaml

## 📁 DAG Deployment (Manual GitOps Style)

Add/Update your DAGs in the dags/ directory

Recreate the ConfigMap:

kubectl delete configmap airflow-dags -n airflow
kubectl create configmap airflow-dags --from-file=dags/ -n airflow

Restart Deployments:

kubectl rollout restart deployment airflow-web -n airflow
kubectl rollout restart deployment airflow-scheduler -n airflow

---

## 🔐 Airflow Login

Credentials are set via the Airflow init job.

🌐 Access Airflow Web UI
Get the LoadBalancer URL:

kubectl get svc airflow-web -n airflow
Open the EXTERNAL-IP in your browser with port 8080.


## 🔧 Enhancements (Optional)

DAG sync using Git sidecar container

Use S3 or PVC for DAG/log persistence

CI/CD with GitHub Actions

Ingress + cert-manager for HTTPS

Monitoring with Prometheus + Grafana
---
## 🙋‍♂️ Author
Gourav Mishra
DevOps Engineer | AWS Certified
🔗 LinkedIn
