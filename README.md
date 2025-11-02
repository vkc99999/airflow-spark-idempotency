tbd# Airflow + Spark on Kubernetes (Minikube Local Demo)
# ---------------------------------------------------
This repo runs Airflow (KubernetesExecutor) on Minikube
and submits Spark-on-K8s jobs through an idempotent operator
that skips duplicate driver pods (no double-submit).

======================================================
1. CLEAN START
======================================================
minikube delete --all --purge
minikube start --driver=docker --cpus=4 --memory=8192

======================================================
2. BUILD AND LOAD IMAGES
======================================================
docker build -t my-airflow:3.0.0-spark-v23 -f docker/Dockerfile.airflow .
docker build -t my-spark:3.5.7-job        -f docker/Dockerfile.spark .
minikube image load my-airflow:3.0.0-spark-v23
minikube image load my-spark:3.5.7-job

======================================================
3. CREATE NAMESPACE + SERVICE ACCOUNTS + RBAC
======================================================
kubectl create namespace airflow
kubectl create serviceaccount airflow-sa -n airflow
kubectl create serviceaccount airflow-worker -n airflow

# Give both driver + worker permission for Spark
kubectl apply -f kubernetes/spark-rbac.yaml -n airflow

======================================================
4. MOUNT LOCAL DAGS AND PLUGINS
======================================================
# Keep both running in separate terminals
minikube mount $(pwd)/dags:/airflow-dags
minikube mount $(pwd)/plugins:/airflow-plugins

======================================================
5. INSTALL AIRFLOW VIA HELM
======================================================
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  -n airflow -f kubernetes/airflow_helm_values.yaml

# Wait till pods are running
kubectl get pods -n airflow

======================================================
6. ACCESS AIRFLOW WEB UI
======================================================
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
# Open http://localhost:8080
# Login using username/password defined in airflow_helm_values.yaml

======================================================
7. VERIFY DAGS + PLUGINS MOUNTED
======================================================
kubectl exec -n airflow -it deploy/airflow-scheduler -- ls -la /opt/airflow/dags
kubectl exec -n airflow -it deploy/airflow-scheduler -- ls -la /opt/airflow/plugins

======================================================
8. RUN THE DEMO DAG
======================================================
# DAG name: spark_idempotency_demo
# Task name: spark_pi
# This uses SparkK8sIdempotentOperator to skip if a driver already runs.

======================================================
9. CHECK SPARK PODS AND LOGS
======================================================
kubectl get pods -n airflow
kubectl logs -n airflow <spark-driver-pod-name>

# Example (view last 50 lines of Spark pod logs)
kubectl logs -n airflow spark-idempotency-demo-spark-pi-xxxxxx-driver --tail=50

10. CHECK EXISTING DRIVER PODS (to confirm idempotency)
======================================================
# Before re-running the DAG, check for active driver pods:
kubectl get pods -n airflow | grep spark

# If you see a driver pod for the same spark.app.name in Running or Pending state,
# the operator will SKIP resubmission automatically.

# To rerun cleanly, delete existing drivers manually:
kubectl delete pod <driver-pod-name> -n airflow

======================================================
11. COMMON FIXES
======================================================
# ❌ Error: "Forbidden! airflow-worker cannot patch services"
kubectl apply -f kubernetes/spark-rbac.yaml -n airflow

# ❌ DAG not found
Ensure minikube mounts are running and airflow pods restarted:
kubectl delete pods --all -n airflow

# ❌ ImportError: No module named 'plugins'
Check plugin folder inside pod:
kubectl exec -n airflow -it deploy/airflow-scheduler -- ls /opt/airflow/plugins

======================================================
12. CLEANUP / RESET (anytime)
======================================================
kubectl delete pods --all -n airflow
minikube delete --all --purge
