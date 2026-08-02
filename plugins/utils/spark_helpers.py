import hashlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

IDEMPOTENCY_LABEL = "airflow-idempotency-key"
ACTIVE_PHASES = frozenset({"pending", "running"})
SUCCESS_PHASES = frozenset({"succeeded"})
FAILURE_PHASES = frozenset({"failed", "unknown"})


@dataclass(frozen=True)
class DriverPod:
    name: str
    phase: str
    created_at: str = ""


def build_idempotency_key(
    context: Mapping[str, Any],
    override: str | None = None,
) -> str:
    if override:
        identity = override
    else:
        dag = context.get("dag")
        task = context.get("task")
        task_instance = context.get("ti")
        dag_id = getattr(dag, "dag_id", "")
        task_id = getattr(task, "task_id", "")
        run_id = context.get("run_id") or getattr(task_instance, "run_id", "")
        map_index = getattr(task_instance, "map_index", -1)
        if not dag_id or not task_id or not run_id:
            raise ValueError("Airflow context is missing dag_id, task_id, or run_id")
        identity = f"{dag_id}|{task_id}|{run_id}|{map_index}"

    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:40]


def _core_v1_api():
    from kubernetes.config.config_exception import ConfigException

    from kubernetes import client, config

    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()
    return client.CoreV1Api()


def _timestamp(value: datetime | str | None) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def list_driver_pods(
    idempotency_key: str,
    namespace: str,
    *,
    api=None,
) -> list[DriverPod]:
    core_api = api or _core_v1_api()
    response = core_api.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"{IDEMPOTENCY_LABEL}={idempotency_key},spark-role=driver",
        _request_timeout=30,
    )
    pods = [
        DriverPod(
            name=item.metadata.name,
            phase=(item.status.phase or "Unknown").lower(),
            created_at=_timestamp(item.metadata.creation_timestamp),
        )
        for item in response.items
    ]
    return sorted(pods, key=lambda pod: pod.created_at, reverse=True)


def classify_driver_pods(
    pods: Sequence[DriverPod],
) -> tuple[str, DriverPod | None]:
    for phases, outcome in (
        (ACTIVE_PHASES, "active"),
        (SUCCESS_PHASES, "succeeded"),
        (FAILURE_PHASES, "failed"),
    ):
        matching = next((pod for pod in pods if pod.phase in phases), None)
        if matching:
            return outcome, matching
    return "missing", None


def wait_for_terminal_phase(
    pod_name: str,
    namespace: str,
    *,
    timeout_seconds: int,
    poll_interval_seconds: int = 10,
    api=None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> str:
    core_api = api or _core_v1_api()
    deadline = monotonic() + timeout_seconds

    while monotonic() < deadline:
        pod = core_api.read_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            _request_timeout=30,
        )
        phase = (pod.status.phase or "Unknown").lower()
        if phase not in ACTIVE_PHASES:
            return phase
        sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Driver pod {pod_name!r} did not finish within {timeout_seconds} seconds"
    )
