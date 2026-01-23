from __future__ import annotations

import os
import re
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

import boto3
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


CFG = {
    "dag_id": "lakehouse_in_clinical_source_load_{{ SOURCE_DATASET }}",
    "tags": [{{ DAG_TAGS }}],
    "schedule": {{ SCHEDULE }},
    "max_active_runs": 1,

    "sensor_poke_interval": 4 * 60 * 60,
    "sensor_timeout": 24 * 60 * 60,

    "aws_conn_id": "aws_default",
    "ods_bucket": Variable.get("ODS_S3_BUCKET"),
    "sdlc_env": Variable.get("SDLC_ENV"),

    "emr_name": "{{ EMR_CONFIG_NAME }}",
    "emr_config_key": "admin/config/BMC_EMR.config",

    "source_dataset": "{{ SOURCE_DATASET }}",

    # Excel-based: landing/source/current/ -> landing/source/<dataset>/
    "source_s3_path": "landing/source/current",
    "target_s3_path": "landing/source/{{ SOURCE_DATASET }}",
    "archive_s3_path": "landing/source/archive/{{ SOURCE_DATASET }}",

    # Excel-based file pattern needs a file prefix (e.g., CodeCollectionDimension)
    "file_prefix": "{{ FILE_PREFIX }}",

    # Control file locations (Excel-based)
    "control_file_path": "raw/source/ctrlfile/ingestion/",
    "control_file_archive_path": "admin/archive/source/ctrlfile/ingestion/",
    "control_file_prefix": "{{ CONTROL_FILE_PREFIX }}",  # e.g., control_file_source OR control_file_<dataset>

    # Keep your EMR scripts AS-IS (paths unchanged)
    "script_landing_to_raw": "/home/hadoop/ods-utilities/landingToRawS3.sh",
    "script_raw_to_sss": "/home/hadoop/batch-process/ods-raw-delta-process.sh",

    # If your existing framework uses these values, keep them templated
    "source_system_name_for_emr": "{{ SOURCE_SYSTEM_NAME }}",          # e.g., source
    "resource_link_name_for_emr": "{{ RESOURCE_LINK_NAME }}",          # e.g., rl_raw_source

    # If your batch/control tasks depend on an internal config file, keep it templated
    "config_file": "{{ ODS_CONFIG_FILE }}",                            # e.g., admin/domains/lakehouse/raw/SOURCE/config/ods.config
}

ODS_BUCKET = CFG["ods_bucket"]


class S3ObjectManager:
    def __init__(self, bucket: str, aws_conn_id: str):
        self.bucket = bucket
        self.s3 = S3Hook(aws_conn_id=aws_conn_id)
        self.client = boto3.client("s3")

    def head(self, key: str) -> Optional[Dict]:
        try:
            return self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            return None

    def validate_source_exists(self, key: str) -> bool:
        return self.head(key) is not None

    def validate_copy(self, source_key: str, destination_key: str) -> Optional[Dict]:
        src = self.head(source_key)
        dst = self.head(destination_key)
        if not src or not dst:
            return None
        if int(src.get("ContentLength", -1)) != int(dst.get("ContentLength", -2)):
            return None
        return {"Size": dst.get("ContentLength"), "ETag": dst.get("ETag")}

    def copy_then_delete(self, source_key: str, destination_prefix: str) -> str:
        filename = source_key.split("/")[-1]
        dest_key = os.path.join(destination_prefix.rstrip("/"), filename)

        if not self.validate_source_exists(source_key):
            raise AirflowFailException(f"Source does not exist: s3://{self.bucket}/{source_key}")

        self.s3.copy_object(
            source_bucket_key=source_key,
            dest_bucket_key=dest_key,
            source_bucket_name=self.bucket,
            dest_bucket_name=self.bucket,
        )

        if not self.validate_copy(source_key, dest_key):
            raise AirflowFailException(f"Copy validation failed: {source_key} -> {dest_key}")

        self.s3.delete_objects(bucket=self.bucket, keys=[source_key])
        return dest_key

    def list_s3_objects_wildcard(self, prefix: str, filename_pattern: str) -> List[str]:
        regex_pattern = (
            filename_pattern.replace(".", r"\.")
            .replace("*", ".*")
            .replace("?", ".")
            + r"$"
        )
        rgx = re.compile(regex_pattern)

        matching: List[str] = []
        continuation_token = None

        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            resp = self.client.list_objects_v2(**params)
            contents = resp.get("Contents", [])
            for obj in contents:
                key = obj["Key"]
                filename = key[len(prefix):] if key.startswith(prefix) else key.split("/")[-1]
                if rgx.match(filename):
                    matching.append(key)

            if not resp.get("IsTruncated"):
                break
            continuation_token = resp.get("NextContinuationToken")

        return matching


def make_emr_step(name: str, script: str, args: List[str]) -> dict:
    return {
        "Name": name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["bash", script] + args,
        },
    }


def _select_batch_from_current(keys: List[str], file_prefix: str) -> Tuple[str, str, List[str]]:
    """
    Returns: (load_type, batch_timestamp, ordered_keys)
    Pattern (Excel): <Prefix>-YYYYMMDDHHMMSS-(Full|Incremental)-N.tsv
    """
    rgx = re.compile(
        rf"^{re.escape(file_prefix)}-(\d{{14}})-(Full|Incremental)-(\d+)\.tsv$"
    )

    candidates = []
    for k in keys:
        fn = k.split("/")[-1]
        m = rgx.match(fn)
        if not m:
            continue
        ts = m.group(1)
        lt = m.group(2)
        seq = int(m.group(3))
        candidates.append((ts, lt, seq, k))

    if not candidates:
        raise AirflowFailException("No matching .tsv files found for the expected pattern.")

    candidates.sort(key=lambda x: (x[0], 0 if x[1] == "Full" else 1, x[2]))
    batch_ts, batch_lt, _, _ = candidates[0]

    batch = [c for c in candidates if c[0] == batch_ts and c[1] == batch_lt]
    batch.sort(key=lambda x: x[2])

    ordered_keys = [b[3] for b in batch]
    return batch_lt, batch_ts, ordered_keys


def _manual_intervention_full() -> None:
    raise AirflowFailException(
        "FULL load detected. Clean up target table manually, then Mark this task SUCCESS to proceed."
    )


def _branch_on_load_type(**kwargs) -> str:
    ti = kwargs["ti"]
    load_type = ti.xcom_pull(task_ids="select_files_batch", key="load_type")
    if load_type == "Full":
        return "manual_intervention_full"
    return "skip_manual_intervention"


def _move_selected_files(**kwargs) -> List[str]:
    ti = kwargs["ti"]
    mgr = S3ObjectManager(bucket=ODS_BUCKET, aws_conn_id=CFG["aws_conn_id"])

    src_prefix = CFG["source_s3_path"].rstrip("/") + "/"
    tgt_prefix = CFG["target_s3_path"].rstrip("/") + "/"

    selected = ti.xcom_pull(task_ids="select_files_batch", key="selected_keys") or []
    if not selected:
        raise AirflowFailException("No selected keys found to move.")

    moved: List[str] = []
    for k in selected:
        if not k.startswith(src_prefix):
            raise AirflowFailException(f"Unexpected key outside current/ prefix: {k}")
        new_key = k.replace(src_prefix, tgt_prefix, 1)

        mgr.s3.copy_object(
            source_bucket_key=k,
            dest_bucket_key=new_key,
            source_bucket_name=ODS_BUCKET,
            dest_bucket_name=ODS_BUCKET,
        )
        if not mgr.validate_copy(k, new_key):
            raise AirflowFailException(f"Move copy validation failed: {k} -> {new_key}")

        mgr.s3.delete_objects(bucket=ODS_BUCKET, keys=[k])
        moved.append(new_key)

    ti.xcom_push(key="moved_tsv_keys", value=moved)
    return moved


def _get_emr_id_runtime() -> str:
    s3 = boto3.resource("s3")
    body = (
        s3.Object(ODS_BUCKET, CFG["emr_config_key"])
        .get()["Body"]
        .read()
        .decode("utf-8")
    )

    matches = []
    for line in body.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) > 3 and parts[0] == CFG["emr_name"].strip():
            matches.append(parts[3])

    if not matches:
        raise AirflowFailException(
            f"EMR cluster '{CFG['emr_name']}' not found in s3://{ODS_BUCKET}/{CFG['emr_config_key']}"
        )
    return matches[0].strip()


def args_landing_to_raw(load_type: str) -> List[str]:
    return [
        CFG["ods_bucket"],
        CFG["source_system_name_for_emr"],
        CFG["source_dataset"],
        load_type.lower(),
        CFG["resource_link_name_for_emr"],
    ]


def args_raw_to_sss(control_file_key: str) -> List[str]:
    return [
        CFG["ods_bucket"],
        CFG["source_system_name_for_emr"],
        CFG["source_dataset"],
        control_file_key,
    ]


def _get_batch_number(**kwargs) -> str:
    from packages.get_batch_number import Get_Batch_number  # noqa: F401

    obj = Get_Batch_number(
        bucket_name=CFG["ods_bucket"],
        source_system=CFG["source_system_name_for_emr"],
        conf=CFG["config_file"],
    )

    batch = obj.get_raw_batch_number()
    if batch is None:
        raise AirflowFailException("Unable to determine batch number.")
    return str(batch)


def _get_control_file(**kwargs) -> str:
    ti = kwargs["ti"]
    batch_number = ti.xcom_pull(task_ids="get_batch_number")
    if not batch_number:
        raise AirflowFailException("Batch number missing.")

    mgr = S3ObjectManager(bucket=ODS_BUCKET, aws_conn_id=CFG["aws_conn_id"])
    prefix = CFG["control_file_path"].rstrip("/") + "/"

    pattern = f"{CFG['control_file_prefix']}_20*_{batch_number}.json"
    matches = mgr.list_s3_objects_wildcard(prefix=prefix, filename_pattern=pattern)

    if not matches:
        raise AirflowFailException(f"Control file not found under {prefix} with pattern {pattern}")

    matches.sort()
    return matches[0]


def _archive_all(**kwargs) -> None:
    ti = kwargs["ti"]
    mgr = S3ObjectManager(bucket=ODS_BUCKET, aws_conn_id=CFG["aws_conn_id"])

    moved_tsv = ti.xcom_pull(task_ids="move_selected_files", key="moved_tsv_keys") or []
    control_file_key = ti.xcom_pull(task_ids="get_control_file")

    if control_file_key:
        mgr.copy_then_delete(
            source_key=control_file_key,
            destination_prefix=CFG["control_file_archive_path"],
        )

    for k in moved_tsv:
        mgr.copy_then_delete(
            source_key=k,
            destination_prefix=CFG["archive_s3_path"],
        )


default_args = {"owner": "data-eng", "retries": 0, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id=CFG["dag_id"],
    default_args=default_args,
    schedule=CFG["schedule"],
    start_date=pendulum.datetime(2025, 12, 1, tz="America/New_York"),
    catchup=False,
    tags=CFG["tags"],
    max_active_runs=CFG["max_active_runs"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="FILE_WATCHER") as file_watcher_group:

        @task.sensor(
            task_id="s3_file_sensor_task",
            mode="reschedule",
            poke_interval=CFG["sensor_poke_interval"],
            timeout=CFG["sensor_timeout"],
        )
        def s3_file_sensor():
            mgr = S3ObjectManager(bucket=ODS_BUCKET, aws_conn_id=CFG["aws_conn_id"])
            src_prefix = CFG["source_s3_path"].rstrip("/") + "/"

            pattern = rf"^{re.escape(CFG['file_prefix'])}-\d{{14}}-(Full|Incremental)-\d+\.tsv$"
            rgx = re.compile(pattern)

            keys = mgr.s3.list_keys(bucket_name=ODS_BUCKET, prefix=src_prefix) or []
            for k in keys:
                if rgx.match(k.split("/")[-1]):
                    return k
            return False

        s3_file_sensor()

    def _select_files_batch(**kwargs) -> None:
        ti = kwargs["ti"]
        mgr = S3ObjectManager(bucket=ODS_BUCKET, aws_conn_id=CFG["aws_conn_id"])
        src_prefix = CFG["source_s3_path"].rstrip("/") + "/"
        keys = mgr.s3.list_keys(bucket_name=ODS_BUCKET, prefix=src_prefix) or []

        load_type, batch_ts, selected = _select_batch_from_current(keys, CFG["file_prefix"])
        ti.xcom_push(key="load_type", value=load_type)
        ti.xcom_push(key="batch_ts", value=batch_ts)
        ti.xcom_push(key="selected_keys", value=selected)

    select_files_batch = PythonOperator(
        task_id="select_files_batch",
        python_callable=_select_files_batch,
    )

    move_selected_files = PythonOperator(
        task_id="move_selected_files",
        python_callable=_move_selected_files,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_load_type",
        python_callable=_branch_on_load_type,
    )

    manual_intervention_full = PythonOperator(
        task_id="manual_intervention_full",
        python_callable=_manual_intervention_full,
    )

    skip_manual_intervention = EmptyOperator(task_id="skip_manual_intervention")

    gate_done = EmptyOperator(
        task_id="gate_done",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    get_emr_id = PythonOperator(
        task_id="get_emr_id",
        python_callable=_get_emr_id_runtime,
    )

    with TaskGroup(group_id="landing_to_raw") as tg_l2r:
        add = EmrAddStepsOperator(
            task_id="add",
            job_flow_id="{{ ti.xcom_pull(task_ids='get_emr_id') }}",
            aws_conn_id=CFG["aws_conn_id"],
            steps=[
                make_emr_step(
                    name=f"Landing->Raw {CFG['source_dataset']}",
                    script=CFG["script_landing_to_raw"],
                    args=args_landing_to_raw("{{ ti.xcom_pull(task_ids='select_files_batch', key='load_type') }}"),
                )
            ],
        )

        wait = EmrStepSensor(
            task_id="wait",
            job_flow_id="{{ ti.xcom_pull(task_ids='get_emr_id') }}",
            aws_conn_id=CFG["aws_conn_id"],
            step_id="{{ ti.xcom_pull(task_ids='landing_to_raw.add', key='return_value')[0] }}",
            poke_interval=60,
            timeout=3600,
        )

        add >> wait

    get_batch_number = PythonOperator(
        task_id="get_batch_number",
        python_callable=_get_batch_number,
    )

    get_control_file = PythonOperator(
        task_id="get_control_file",
        python_callable=_get_control_file,
    )

    with TaskGroup(group_id="raw_to_sss") as tg_r2s:
        add = EmrAddStepsOperator(
            task_id="add",
            job_flow_id="{{ ti.xcom_pull(task_ids='get_emr_id') }}",
            aws_conn_id=CFG["aws_conn_id"],
            steps=[
                make_emr_step(
                    name=f"Raw->SSS {CFG['source_dataset']}",
                    script=CFG["script_raw_to_sss"],
                    args=args_raw_to_sss("{{ ti.xcom_pull(task_ids='get_control_file') }}"),
                )
            ],
        )

        wait = EmrStepSensor(
            task_id="wait",
            job_flow_id="{{ ti.xcom_pull(task_ids='get_emr_id') }}",
            aws_conn_id=CFG["aws_conn_id"],
            step_id="{{ ti.xcom_pull(task_ids='raw_to_sss.add', key='return_value')[0] }}",
            poke_interval=60,
            timeout=3600,
        )

        add >> wait

    archive_all = PythonOperator(
        task_id="archive_all",
        python_callable=_archive_all,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> file_watcher_group >> select_files_batch >> move_selected_files >> branch
    branch >> manual_intervention_full
    branch >> skip_manual_intervention
    [manual_intervention_full, skip_manual_intervention] >> gate_done

    gate_done >> get_emr_id >> tg_l2r >> get_batch_number >> get_control_file >> tg_r2s >> archive_all >> end