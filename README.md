```bash
# Airflow + Spark on Kubernetes (Minikube Local Demo)
# Runs Airflow (KubernetesExecutor) on Minikube and submits Spark-on-K8s jobs
# through an idempotent operator that skips duplicate driver pods.

# --- CLEAN START ---
minikube delete --all --purge
minikube start --driver=docker --cpus=4 --memory=8192

# --- BUILD AND LOAD IMAGES ---
docker build -t my-airflow:3.0.0-spark-v23 -f docker/Dockerfile.airflow .
docker build -t my-spark:3.5.7-job -f docker/Dockerfile.spark .
minikube image load my-airflow:3.0.0-spark-v23
minikube image load my-spark:3.5.7-job

# --- CREATE NAMESPACE + SERVICE ACCOUNTS + RBAC ---
kubectl create namespace airflow
kubectl create serviceaccount airflow-sa -n airflow
kubectl apply -f kubernetes/spark-rbac.yaml -n airflow

# --- DEPLOY AIRFLOW ---
helm upgrade --install airflow apache-airflow/airflow -n airflow \
  -f kubernetes/airflow_helm_values.yaml

# --- VERIFY PODS ---
kubectl get pods -n airflow
kubectl delete pods --all -n airflow  # optional reset

# --- PORT FORWARD WEB UI ---
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow

# --- VIEW SPARK DRIVER LOGS ---
kubectl logs -n airflow <driver-pod-name>

# --- INSPECT AIRFLOW XCOM (if used) ---
kubectl exec -it -n airflow $(kubectl get pod -n airflow \
  -l component=scheduler -o jsonpath='{.items[0].metadata.name}') -- \
  psql -h host.minikube.internal -U airflow -d airflow \
  -c "select * from xcom where dag_id='spark_idempotency_demo';"
