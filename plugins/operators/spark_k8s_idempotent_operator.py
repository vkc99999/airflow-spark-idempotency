from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.spark_helpers import (
    FAILURE_PHASES,
    IDEMPOTENCY_LABEL,
    build_idempotency_key,
    classify_driver_pods,
    list_driver_pods,
    wait_for_terminal_phase,
)


class SparkK8sIdempotentOperator(SparkSubmitOperator):
    """Suppress duplicate Spark driver submissions within one Airflow run."""

    template_fields = tuple(
        dict.fromkeys((*SparkSubmitOperator.template_fields, "idempotency_key"))
    )

    def __init__(
        self,
        *,
        conf: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        existing_driver_timeout: int = 3600,
        **kwargs,
    ):
        super().__init__(conf=dict(conf or {}), **kwargs)
        self.idempotency_key = idempotency_key
        self.existing_driver_timeout = existing_driver_timeout

    def _namespace(self) -> str:
        return self.conf.get("spark.kubernetes.namespace", "airflow")

    def _label_submission(self, key: str) -> None:
        self.conf[f"spark.kubernetes.driver.label.{IDEMPOTENCY_LABEL}"] = key
        self.conf[f"spark.kubernetes.executor.label.{IDEMPOTENCY_LABEL}"] = key

    def _reuse_existing_driver(self, key: str, namespace: str):
        outcome, pod = classify_driver_pods(list_driver_pods(key, namespace))
        if outcome == "missing" or outcome == "failed":
            if pod:
                self.log.info(
                    "Previous driver %s ended in %s; allowing the Airflow retry to submit again",
                    pod.name,
                    pod.phase,
                )
            return None

        if outcome == "succeeded":
            self.log.info("Driver %s already succeeded; reusing its result", pod.name)
            return {
                "status": "already_succeeded",
                "driver_pod": pod.name,
                "idempotency_key": key,
            }

        self.log.info(
            "Driver %s is %s; waiting instead of submitting a duplicate",
            pod.name,
            pod.phase,
        )
        phase = wait_for_terminal_phase(
            pod.name,
            namespace,
            timeout_seconds=self.existing_driver_timeout,
        )
        if phase in FAILURE_PHASES:
            raise AirflowException(
                f"Existing driver {pod.name!r} finished in phase {phase!r}"
            )
        if phase != "succeeded":
            raise AirflowException(
                f"Existing driver {pod.name!r} finished in unsupported phase {phase!r}"
            )
        return {
            "status": "reattached_succeeded",
            "driver_pod": pod.name,
            "idempotency_key": key,
        }

    def execute(self, context):
        key = build_idempotency_key(context, self.idempotency_key)
        namespace = self._namespace()
        self._label_submission(key)

        existing_result = self._reuse_existing_driver(key, namespace)
        if existing_result is not None:
            return existing_result

        self.log.info(
            "No successful or active driver found for idempotency key %s; submitting",
            key,
        )
        result = super().execute(context)

        outcome, pod = classify_driver_pods(list_driver_pods(key, namespace))
        if outcome == "failed":
            raise AirflowException(
                f"Submitted driver {pod.name!r} finished in phase {pod.phase!r}"
            )
        self.log.info(
            "Spark submission completed; observed driver outcome=%s pod=%s",
            outcome,
            pod.name if pod else "not-retained",
        )
        return result
