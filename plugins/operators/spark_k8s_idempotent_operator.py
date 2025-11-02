import time
from typing import Any, Dict, Optional
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.spark_helpers import _run_cmd, _find_driver_once, _list_driver_pods, _pod_phase


class SparkK8sIdempotentOperator(SparkSubmitOperator):
    template_fields = ("conf",)

    def __init__(self, conf: Optional[Dict[str, Any]] = None, *args, **kwargs):
        conf = dict(conf or {})
        super().__init__(conf=conf, *args, **kwargs)
        self.conf = conf

    def _get_namespace(self) -> str:
        return self.conf.get("spark.kubernetes.namespace", "airflow")

    def _spark_app_name(self) -> str:
        if isinstance(getattr(self, "conf", None), dict):
            n = self.conf.get("spark.app.name")
            if n:
                return n
        return getattr(self, "name", None) or "spark-app"

    def execute(self, context):
        app_name = self._spark_app_name()
        ns = self._get_namespace()

        # --- Check for existing running/pending driver ---
        print(f"[DBG] Checking for existing Spark driver pods for app={app_name} ns={ns}")
        candidates = _list_driver_pods(app_name, ns)
        if candidates:
            for pod in candidates:
                phase = _pod_phase(pod, ns).lower()
                if phase in ("running", "pending"):
                    raise AirflowSkipException(
                        f"Driver '{pod}' already {phase}. Skipping resubmit."
                    )
                elif phase == "succeeded":
                    print(f"[DBG] Found completed driver {pod}, phase={phase}. Safe to rerun if needed.")

        # --- Submit Spark job ---
        print("[DBG] No live driver pods detected. Proceeding to spark-submit.")
        res = super().execute(context)
        print(f"[DBG] spark-submit returned: {res}")

        # --- Post-submit monitoring ---
        try:
            driver_pod = _find_driver_once(app_name, ns)
            if not driver_pod:
                raise AirflowException("No driver pod detected post-submit.")
            phase = _pod_phase(driver_pod, ns)
            print(f"[DBG] Final driver phase={phase}")
            if phase == "Failed":
                raise AirflowException(f"Driver '{driver_pod}' failed after submission.")
        except AirflowException:
            raise
        except Exception as e:
            print(f"[DBG] Final pod check failed: {e}")

        return res
