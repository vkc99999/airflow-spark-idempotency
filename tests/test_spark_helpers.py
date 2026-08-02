from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from utils.spark_helpers import (
    DriverPod,
    build_idempotency_key,
    classify_driver_pods,
    list_driver_pods,
    wait_for_terminal_phase,
)


def airflow_context(run_id="scheduled__2026-08-02", map_index=-1, try_number=1):
    return {
        "dag": SimpleNamespace(dag_id="spark_idempotency_demo"),
        "task": SimpleNamespace(task_id="spark_pi"),
        "run_id": run_id,
        "ti": SimpleNamespace(
            run_id=run_id,
            map_index=map_index,
            try_number=try_number,
        ),
    }


def pod(name, phase, created_at):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, creation_timestamp=created_at),
        status=SimpleNamespace(phase=phase),
    )


class FakeCoreApi:
    def __init__(self, listed=(), phases=()):
        self.listed = list(listed)
        self.phases = iter(phases)
        self.selector = None

    def list_namespaced_pod(self, namespace, label_selector, _request_timeout):
        self.selector = (namespace, label_selector, _request_timeout)
        return SimpleNamespace(items=self.listed)

    def read_namespaced_pod(self, name, namespace, _request_timeout):
        return SimpleNamespace(status=SimpleNamespace(phase=next(self.phases)))


def test_idempotency_key_is_stable_across_retries():
    first = build_idempotency_key(airflow_context(try_number=1))
    retry = build_idempotency_key(airflow_context(try_number=4))

    assert first == retry
    assert len(first) == 40


def test_idempotency_key_changes_for_run_and_map_index():
    base = build_idempotency_key(airflow_context())

    assert build_idempotency_key(airflow_context(run_id="manual__other")) != base
    assert build_idempotency_key(airflow_context(map_index=0)) != base


def test_list_driver_pods_uses_key_and_sorts_newest_first():
    old = datetime(2026, 8, 1, tzinfo=timezone.utc)
    new = datetime(2026, 8, 2, tzinfo=timezone.utc)
    api = FakeCoreApi(
        listed=[pod("old-driver", "Succeeded", old), pod("new-driver", "Running", new)]
    )

    result = list_driver_pods("abc123", "airflow", api=api)

    assert [item.name for item in result] == ["new-driver", "old-driver"]
    assert result[0].phase == "running"
    assert api.selector == (
        "airflow",
        "airflow-idempotency-key=abc123,spark-role=driver",
        30,
    )


def test_classification_prioritizes_an_active_duplicate():
    pods = [
        DriverPod("completed", "succeeded", "2026-08-01"),
        DriverPod("active", "running", "2026-08-02"),
    ]

    assert classify_driver_pods(pods) == ("active", pods[1])
    assert classify_driver_pods([pods[0]]) == ("succeeded", pods[0])
    assert classify_driver_pods([]) == ("missing", None)


def test_wait_for_terminal_phase_reattaches_until_success():
    api = FakeCoreApi(phases=["Running", "Succeeded"])
    clock = iter([0, 0, 1])

    phase = wait_for_terminal_phase(
        "driver",
        "airflow",
        timeout_seconds=10,
        poll_interval_seconds=0,
        api=api,
        sleep=lambda _seconds: None,
        monotonic=lambda: next(clock),
    )

    assert phase == "succeeded"


def test_wait_for_terminal_phase_times_out_before_resubmitting():
    api = FakeCoreApi(phases=["Running"])
    clock = iter([0, 11])

    with pytest.raises(TimeoutError, match="did not finish"):
        wait_for_terminal_phase(
            "driver",
            "airflow",
            timeout_seconds=10,
            api=api,
            sleep=lambda _seconds: None,
            monotonic=lambda: next(clock),
        )
