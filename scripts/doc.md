 Airflow Task Templates Documentation (Additions)

## 1. `send_failure_email`

### Description
Sends **one email** if **any task** in the current DAG run fails.  
It picks the right email list based on `SDLC_ENV` (DEV / ST / RT / PROD) and includes links to failed task logs.

### Template File
`send_failure_email`

#### Input parameters need to pass in
1. `task-template-name` should be `send_failure_email`
2. `input-params-in-order` should include the parameters below

### Required Variables

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `emails_prod` | String | Comma-separated recipients for PROD | `"dl-prod@x.org, oncall@x.org"` |
| `emails_st` | String | Comma-separated recipients for ST | `"dl-st@x.org"` |
| `emails_rt` | String | Comma-separated recipients for RT | `"dl-rt@x.org"` |
| `emails_dev` | String | Comma-separated recipients for DEV (default) | `"you@x.org"` |
| `method` | String | Function name used by the generator | `"send_failure_email"` |

### Return Value
None. It sends email only when there is at least one **FAILED** task in the run.

### Usage Example
```json
{
  "task-group-name": "NOTIFY_FAILURE",
  "dependency-group-name": ["ARCHIVE_CONTROL_FILE"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "send_failure_email",
      "input-params-in-order": {
        "emails_prod": "dl-prod@x.org",
        "emails_st": "dl-st@x.org",
        "emails_rt": "dl-rt@x.org",
        "emails_dev": "you@x.org",
        "method": "send_failure_email"
      }
    }
  ]
}
```

### Notes
- Uses `trigger_rule="all_done"` so it can run even when upstream tasks fail.
- It lists tasks with state **FAILED** only (skipped tasks are not treated as failures).
- If the recipients list for the current env is empty, it logs and returns (no email).
- If there are **0 failed tasks**, it logs and returns (no email).

---

## 2. `get_s3_file_unzip`

### Description
Finds the next `*.tsv.gz` file in `landing/source/current/`, picks the next file in **FIFO order**, copies it to the dataset folder, unzips it there into a `.tsv`, and returns the `.tsv` S3 key for downstream tasks.

**Selection rules (FIFO + safe Full priority):**
1. Look at filenames like:  
   `<prefix>-YYYYMMDDHHMMSS-(Full|Incremental)-N.tsv.gz`
2. Pick the **oldest date** (`YYYYMMDD`) across all matching files.
3. Within that date:
   - If any **Full** exists for that date, pick **Full** first.
   - Otherwise pick the **oldest timestamp** for that date.
4. If multiple parts exist for the chosen timestamp/type, pick **smallest part number** (`-1`, then `-2`, ...).

### Template File
`get_s3_file_unzip`

#### Input parameters need to pass in
1. `task-template-name` should be `get_s3_file_unzip`
2. `input-params-in-order` should include the parameters below

### Required Variables

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `bucket_name` | String | S3 bucket name (can be `BUCKET_NAME`) | `"BUCKET_NAME"` |
| `source_location` | String | Source prefix that holds new gzip files | `"landing/source/current/"` |
| `target_location` | String | Dataset prefix where file will be copied + unzipped | `"landing/source/<dataset>/"` |
| `file_prefix` | String | Must match the start of the filename exactly | `"CompositeDimension"` |
| `method` | String | Function name used by the generator | `"get_s3_file_unzip"` |

### Return Value
Returns the **unzipped `.tsv` key** (string). This value is pushed to XCom as `return_value` and can be used by downstream templates.

### Usage Example
```json
{
  "task-group-name": "GET_FILE_SOURCE_DATASET_LANDING",
  "dependency-group-name": ["FILE_WATCHER_SOURCE"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "get_s3_file_unzip",
      "input-params-in-order": {
        "method": "get_s3_file_unzip",
        "bucket_name": "BUCKET_NAME",
        "source_location": "landing/source/current/",
        "target_location": "landing/source/composite_dim/",
        "file_prefix": "CompositeDimension"
      }
    }
  ]
}
```

### Notes
- Unzips **in the target folder** (same bucket), so downstream reads are simple.
- Deletes the `.tsv.gz` after successful unzip (keeps S3 clean).
- Uses streaming + size validation (uploaded `.tsv` size must match streamed bytes).
- Supports multi-part files by always picking `-1`, then `-2`, etc across runs.

---

## 3. `emr-execution`

### Description
Runs a shell script on an **EMR cluster** by adding a step and waiting for it to finish.  
Used for batch scripts like `landingToRawS3.sh`, restart scripts, etc.

### Template File
`emr-execution`

#### Input parameters need to pass in
1. `task-template-name` should be `emr-execution`
2. `input-params-in-order` should include the parameters below

### Required Variables

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `params` | String | Script path + args as a single string | `"/home/hadoop/x.sh, BUCKET_NAME, source, table"` |
| `id` | String | TaskGroup id / step group id | `"landing_to_raw"` |
| `method` | String | Function name used by the generator | `"landing_to_raw"` |
| `pass_batch_number` | String | `"Y"` to inject batch number into params | `"N"` |
| `pass_control_file` | String | `"Y"` to inject control file into params | `"N"` |

### Return Value
Returns EMR step id list internally (via XCom in the template). Most DAGs donât use it directly.

### Usage Example
```json
{
  "task-group-name": "LANDING_TO_RAW",
  "dependency-group-name": ["GET_FILE_SOURCE_DATASET_LANDING"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "emr-execution",
      "input-params-in-order": {
        "params": "/home/hadoop/ods-utilities/landingToRawS3.sh, BUCKET_NAME, source, composite_dim, incremental, rl_raw_source",
        "id": "landing_to_raw",
        "method": "landing_to_raw",
        "pass_batch_number": "N",
        "pass_control_file": "N"
      }
    }
  ]
}
```

### Notes
- Adds EMR step, then sensor waits for completion.
- Script args are passed in the `params` string as shown above.
- EMR cluster is resolved using repo utilities (based on bucket config).

---

## 4. `emr-batch-trigger-sensor`

### Description
Watches for a **trigger sensor file** in S3 (example: `.../sensor.txt`).  
When it finds the file, it deletes it and unblocks downstream tasks. This is used for **microbatch** workflows.

### Template File
`emr-batch-trigger-sensor`

#### Input parameters need to pass in
1. `task-template-name` should be `emr-batch-trigger-sensor`
2. `input-params-in-order` should include the parameters below

### Required Variables

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `id` | String | TaskGroup id | `"file_sensor"` |
| `method` | String | Function name used by the generator | `"file_sensor"` |
| `process_type` | String | Process type folder used in the trigger path | `"sss"` |
| `group_name` | String | Group name folder used in the trigger path | `"MHS_CUST_LAKE_GROUP_1"` |

### Return Value
Returns when the trigger file is found (sensor success). Nothing else is returned.

### Usage Example
```json
{
  "task-group-name": "S3_FILE_SENSOR",
  "dependency-group-name": "",
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "emr-batch-trigger-sensor",
      "input-params-in-order": {
        "id": "file_sensor",
        "method": "file_sensor",
        "process_type": "sss",
        "group_name": "MHS_CUST_LAKE_GROUP_1"
      }
    }
  ]
}
```

### Notes
- The trigger key is built as: `delta-lake/work-space/airflow/{process_type}/{group_name}/sensor.txt`
- This is **microbatch** style (often every 15 minutes in these DAGs).
- It deletes the trigger file on success so the next run can wait again.

---

## 5. `emr-serverless-execution-stream-sss`

### Description
Submits and monitors an **EMR Serverless** Spark job for Stream SSS.  
It runs the driver from a ZIP stored in S3 and waits until the job finishes.

### Template File
`emr-serverless-execution-stream-sss`

#### Input parameters need to pass in
1. `task-template-name` should be `emr-serverless-execution-stream-sss`
2. `input-params-in-order` should include the parameters below

### Required Variables

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `id` | String | TaskGroup id | `"sss_load"` |
| `method` | String | Function name used by the generator | `"sss_load"` |
| `group_name` | String | Logical group name for the job | `"MHS_CUST_LAKE_GROUP_1"` |
| `resource_link_name` | String | Resource link used to resolve serverless config | `"rl_raw_mhs"` |
| `base_script_path` | String | Base S3 path where driver artifacts live | `"admin/scripts/emr/home/hadoop/stream-raw"` |
| `spark_conf` | String | Spark submit parameters | `"--driver-memory 3g --executor-memory 13g --num-executors 1 --executor-cores 2"` |

### Return Value
Returns when the EMR Serverless job finishes (success or failure). Nothing else is returned.

### Usage Example
```json
{
  "task-group-name": "MHS_CUST_LAKE_GROUP_1_LOAD",
  "dependency-group-name": ["S3_FILE_SENSOR"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "emr-serverless-execution-stream-sss",
      "input-params-in-order": {
        "id": "sss_load",
        "method": "sss_load",
        "group_name": "MHS_CUST_LAKE_GROUP_1",
        "resource_link_name": "rl_raw_mhs",
        "base_script_path": "admin/scripts/emr/home/hadoop/stream-raw",
        "spark_conf": "--driver-memory 3g --executor-memory 13g --num-executors 1 --executor-cores 2"
      }
    }
  ]
}
```

### Notes
- Uses EMR Serverless StartJob + sensor to wait for completion.
- Runs Spark driver from S3 ZIP (stored under `base_script_path`).
- Standard for Stream SSS jobs in this repo.

---

## 6. `placing_s3_sensor_object`

### Description
Creates/updates the **microbatch sensor file** in S3 so upstream producers can trigger the next cycle, or so downstream jobs can detect completion.

### Template File
`placing_s3_sensor_object`

#### Input parameters need to pass in
1. `task-template-name` should be `placing_s3_sensor_object`
2. `input-params-in-order` should include the parameters below

### Required Variables

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `id` | String | TaskGroup id | `"place_sensor_file"` |
| `method` | String | Function name used by the generator | `"place_sensor_file"` |
| `microbatch_sensor_file` | String | Full key/path of the sensor file to write | `"delta-lake/work-space/airflow/sss/MHS_CUST_LAKE_GROUP_1/sensor.txt"` |

### Return Value
None. It writes the sensor file and returns.

### Usage Example
```json
{
  "task-group-name": "PLACE_SENSOR_FILE",
  "dependency-group-name": ["MHS_CUST_LAKE_GROUP_1_LOAD"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "placing_s3_sensor_object",
      "input-params-in-order": {
        "id": "place_sensor_file",
        "method": "place_sensor_file",
        "microbatch_sensor_file": "delta-lake/work-space/airflow/sss/MHS_CUST_LAKE_GROUP_1/sensor.txt"
      }
    }
  ]
}
```

### Notes
- Used to coordinate microbatch runs (handshake between steps / producers / consumers).
- Commonly placed after a successful load to signal âready for next cycleâ.
