# Task Template Docs (Fixed)

## 1. `s3_file_sensor_custom`

### **Description**
Waits for a file to show up in S3 before starting the next tasks.  
It checks the S3 key (or wildcard pattern) on a schedule until it finds a match or times out.  
This is used as the “file watcher” step at the start of a DAG.

### **Template File**
`s3_file_sensor_custom`

### **Input parameters need to pass in**
1. `task-template-name` should be `s3_file_sensor_custom`
2. `input-params-in-order` should include the inputs below

### **Required Variables**

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `method` | String | Task method name | `s3_file_sensor_task` |
| `bucket_name` | String | Bucket variable reference | `BUCKET_NAME` |
| `s3_key` | String | Key or wildcard pattern to watch | `landing/source/current/CompositeDimension*.tsv.gz` |
| `poke_interval` | Number | Seconds between checks | `60` |
| `timeout` | Number | Total seconds before it fails | `3600` |

### **Return Value**
Usually returns the matched S3 key (string) in XCom (depends on your template implementation).

### **Usage Example**
```json
{
  "task-group-name": "FILE_WATCHER_SOURCE",
  "dependency-group-name": "",
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "s3_file_sensor_custom",
      "input-params-in-order": {
        "method": "s3_file_sensor_task",
        "bucket_name": "BUCKET_NAME",
        "s3_key": "landing/source/current/CompositeDimension*.tsv.gz",
        "poke_interval": 60,
        "timeout": 3600
      }
    }
  ]
}
```

### **Notes**
- `bucket_name` must map to the same bucket where the file lands.
- If you use wildcards, make sure the wildcard matches what the producer really writes (case-sensitive in S3).

---

## 2. `copy_between_s3_buckets`

### **Description**
Copies one or more files from a source bucket/prefix into a target bucket/prefix.  
This is usually used for landing/ingestion when files arrive in one bucket but processing happens in another.  
Optionally, you can also copy to an archive location (depends on how your repo’s template is written).

### **Template File**
`copy_between_s3_buckets`

### **Input parameters need to pass in**
1. `task-template-name` should be `copy_between_s3_buckets`
2. `input-params-in-order` should include the inputs below

### **Required Variables**

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `task_id` | String | Airflow task id | `copy_files` |
| `method` | String | Task method name | `copy_between_s3_buckets` |
| `bucket_name` | String | Bucket variable reference (used for creds/client) | `BUCKET_NAME` |
| `source_bucket` | String | Source bucket name | `incoming-bucket` |
| `source_prefix` | String | Source prefix/folder | `incoming/path/` |
| `target_bucket` | String | Target bucket name | `ods-bucket` |
| `target_path` | String | Target prefix/folder | `landing/source/current/` |
| `pattern` | String | File pattern to include | `*.tsv.gz` |

### **Return Value**
Usually none. Some implementations return a list of copied keys.

### **Usage Example**
```json
{
  "task-group-name": "COPY_TO_LANDING",
  "dependency-group-name": ["FILE_WATCHER_SOURCE"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "copy_between_s3_buckets",
      "input-params-in-order": {
        "task_id": "copy_files",
        "method": "copy_between_s3_buckets",
        "bucket_name": "BUCKET_NAME",
        "source_bucket": "incoming-bucket",
        "source_prefix": "incoming/path/",
        "target_bucket": "ods-bucket",
        "target_path": "landing/source/current/",
        "pattern": "*.tsv.gz"
      }
    }
  ]
}
```

### **Notes**
- If the source and target are the same bucket, this still works (it is just a copy inside the bucket).
- S3 keys are case-sensitive. Prefix must match exactly.

---

## 3. `generate_custom_control_file`

### **Description**
Builds the control file key for the current batch and returns it for downstream tasks.  
This is used between “landingToRaw” and “rawToSSS” when the framework generates a control file and the next step needs the exact key.

### **Template File**
`generate_custom_control_file`

### **Input parameters need to pass in**
1. `task-template-name` should be `generate_custom_control_file`
2. `input-params-in-order` should include the inputs below

### **Required Variables**

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `method` | String | Task method name | `get_control_file` |
| `bucket_name` | String | Bucket variable reference | `BUCKET_NAME` |
| `source_system` | String | Source system name used in control naming | `source` |
| `config_file` | String | Config used to locate control files | `admin/domains/lakehouse/raw/SOURCE/config/ods.config` |
| `file_prefix` | String | Control file prefix | `control_file_source` |
| `control_file_path` | String | Folder where control files are written | `raw/source/ctrlfile/ingestion/` |

### **Return Value**
Returns the control file key (string) in XCom.

### **Usage Example**
```json
{
  "task-group-name": "GET_CONTROL_FILE",
  "dependency-group-name": ["GET_BATCH_NUMBER"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "generate_custom_control_file",
      "input-params-in-order": {
        "method": "get_control_file",
        "bucket_name": "BUCKET_NAME",
        "source_system": "source",
        "config_file": "admin/domains/lakehouse/raw/SOURCE/config/ods.config",
        "file_prefix": "control_file_source",
        "control_file_path": "raw/source/ctrlfile/ingestion/"
      }
    }
  ]
}
```

### **Notes**
- This template usually depends on an upstream batch-number task. If batch number is missing, it will not find the control file.
- Control file names are usually batch-based (example: includes timestamp and batch number).

---

## 4. `archive_s3_source_custom`

### **Description**
Moves (archives) the processed source file into an archive folder in S3.  
It normally reads the source key from XCom (commonly from `get_s3_file`), then copies it to the archive location and deletes the original.

### **Template File**
`archive_s3_source_custom`

### **Input parameters need to pass in**
1. `task-template-name` should be `archive_s3_source_custom`
2. `input-params-in-order` should include the inputs below

### **Required Variables**

| Variable Name | Type | Description | Example |
|---|---|---|---|
| `method` | String | Task method name | `archive_source_file` |
| `bucket_name` | String | Bucket variable reference | `BUCKET_NAME` |
| `target_location` | String | Archive folder/prefix | `landing/source/archive/composite_dim/` |

### **Return Value**
None.

### **Usage Example**
```json
{
  "task-group-name": "ARCHIVE_SOURCE_FILE",
  "dependency-group-name": ["RAW_TO_SSS"],
  "type": "TASK",
  "task-object-type": "method",
  "task-details": [
    {
      "task-template-name": "archive_s3_source_custom",
      "input-params-in-order": {
        "method": "archive_source_file",
        "bucket_name": "BUCKET_NAME",
        "target_location": "landing/source/archive/composite_dim/"
      }
    }
  ]
}
```

### **Notes**
- This template expects the upstream task to return the S3 key of the source file (often `get_s3_file` return value).
- Archive location must be writable by the Airflow role.
