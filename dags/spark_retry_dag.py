from datetime import datetime, timedelta
from airflow import DAG
from operators.spark_k8s_idempotent_operator import SparkK8sIdempotentOperator

# ---------- Config ----------
default_args = {
    "owner": "krishna",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=20),
}

# ---------- DAG ----------
with DAG(
    dag_id="spark_idempotency_demo",
    default_args=default_args,
    description="Run Spark on K8s with DAG-global idempotency",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["spark", "k8s", "idempotent"],
) as dag:
    spark_task = SparkK8sIdempotentOperator(
        task_id="spark_pi",
        name="spark-pi-demo",
        application="local:///opt/spark/work-dir/pi.py",
        conn_id="spark_k8s",
        verbose=True,
        conf={
            "spark.app.name": "spark-pi-demo",
            "spark.kubernetes.namespace": "airflow",
            "spark.executor.instances": "2",
            "spark.kubernetes.container.image": "my-spark:3.5.7-job",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-sa",
            "spark.kubernetes.file.upload.path": "file:///tmp",
        },
    )
