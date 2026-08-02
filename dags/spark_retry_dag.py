from datetime import datetime, timedelta, timezone

from airflow import DAG
from operators.spark_k8s_idempotent_operator import SparkK8sIdempotentOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}


with DAG(
    dag_id="spark_idempotency_demo",
    default_args=default_args,
    description="Run-scoped duplicate suppression for Spark on Kubernetes retries",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    schedule=None,
    max_active_runs=2,
    tags=["spark", "kubernetes", "retries"],
) as dag:
    SparkK8sIdempotentOperator(
        task_id="spark_pi",
        name="spark-pi-demo",
        application="local:///opt/spark/work-dir/pi.py",
        application_args=["--iterations", "30", "--sleep-seconds", "2"],
        conn_id="spark_k8s",
        verbose=True,
        existing_driver_timeout=900,
        conf={
            "spark.app.name": "spark-pi-demo",
            "spark.kubernetes.namespace": "airflow",
            "spark.executor.instances": "2",
            "spark.kubernetes.container.image": "my-spark:3.5.7-job",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow-sa",
            "spark.kubernetes.submission.waitAppCompletion": "true",
        },
    )
