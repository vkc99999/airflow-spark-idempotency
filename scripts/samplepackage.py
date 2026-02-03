import io
import re
import gzip
import hashlib
from typing import Dict, List, Optional, Tuple

import boto3


class _GzipStreamingReader(io.RawIOBase):
    """
    Streams uncompressed bytes from an S3 Body stream.
    Tracks bytes_out + sha256 of the uncompressed TSV.
    """
    def __init__(self, s3_body_stream):
        super().__init__()
        self._gz = gzip.GzipFile(fileobj=s3_body_stream)
        self.bytes_out = 0
        self.sha256 = hashlib.sha256()

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        data = self._gz.read(n)
        if data:
            self.bytes_out += len(data)
            self.sha256.update(data)
        return data


class S3_Object_Management:
    def __init__(self, bucket_name: str):
        self.bucket = bucket_name
        self.s3_client = boto3.client("s3")

    def _list_keys_under_prefix(self, prefix: str) -> List[str]:
        prefix = (prefix or "").lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        keys: List[str] = []
        token = None

        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                params["ContinuationToken"] = token

            resp = self.s3_client.list_objects_v2(**params)
            for obj in resp.get("Contents") or []:
                keys.append(obj["Key"])

            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

        return keys

    def _head(self, key: str) -> dict:
        return self.s3_client.head_object(Bucket=self.bucket, Key=key)

    def _copy_validate_delete(self, source_key: str, dest_key: str) -> None:
        src = self._head(source_key)
        self.s3_client.copy_object(
            CopySource={"Bucket": self.bucket, "Key": source_key},
            Bucket=self.bucket,
            Key=dest_key,
        )
        dst = self._head(dest_key)

        if int(src.get("ContentLength", -1)) != int(dst.get("ContentLength", -2)):
            raise Exception(
                f"Copy size mismatch: {source_key} -> {dest_key} "
                f"(src={src.get('ContentLength')}, dst={dst.get('ContentLength')})"
            )

        self.s3_client.delete_object(Bucket=self.bucket, Key=source_key)

    def _parse_ts_part(self, filename: str, file_prefix: str) -> Optional[Tuple[str, int]]:
        """
        Accepts: <prefix>-<14digitTs>-<anything>-<part>.tsv.gz
        Example: PricerDimension-20251205140420-Incremental-2.tsv.gz
        Returns: (ts, part)
        """
        rx = re.compile(
            rf"^{re.escape(file_prefix)}-(\d{{14}})-[^-]+-(\d+)\.tsv\.gz$"
        )
        m = rx.match(filename)
        if not m:
            return None
        ts = m.group(1)
        part = int(m.group(2))
        return ts, part

    def move_gz_to_dataset_and_unzip_oldest_first(
        self,
        source_location: str,
        target_location: str,
        file_prefix: str,
        delete_gz_after_unzip: bool = True,
    ) -> Dict[str, str]:
        """
        Oldest timestamp first.
        If multiple files share same timestamp => smallest part wins.
        If part-1 is missing, we STILL allow picking part-2/5/etc (as requested).

        Flow:
          - pick next gz from source_location
          - copy to target_location (same filename) + validate + delete from source
          - unzip in target_location to .tsv (streaming)
          - validate TSV size == streamed bytes
          - optionally delete the gz in target
        """
        src_prefix = (source_location or "").strip("/")
        tgt_prefix = (target_location or "").strip("/")
        if src_prefix and not src_prefix.endswith("/"):
            src_prefix += "/"
        if tgt_prefix and not tgt_prefix.endswith("/"):
            tgt_prefix += "/"

        all_keys = self._list_keys_under_prefix(src_prefix)

        candidates: List[Tuple[str, int, str]] = []
        for k in all_keys:
            fn = k.split("/")[-1]
            if not fn.endswith(".tsv.gz"):
                continue
            meta = self._parse_ts_part(fn, file_prefix)
            if not meta:
                continue
            ts, part = meta
            candidates.append((ts, part, k))

        if not candidates:
            raise Exception(
                f"No matching .tsv.gz files under s3://{self.bucket}/{src_prefix} "
                f"for prefix={file_prefix}"
            )

        # Pick oldest timestamp, then smallest part
        oldest_ts = min(ts for (ts, _, _) in candidates)
        group = [(part, key) for (ts, part, key) in candidates if ts == oldest_ts]
        part, selected_src_key = min(group, key=lambda x: x[0])

        filename_gz = selected_src_key.split("/")[-1]
        dest_gz_key = f"{tgt_prefix}{filename_gz}"

        # Move gz: current -> dataset (copy+validate+delete)
        self._copy_validate_delete(selected_src_key, dest_gz_key)

        # Unzip gz in dataset -> tsv in dataset
        dest_tsv_key = dest_gz_key[:-3]  # drop ".gz"

        # If TSV already exists (retry scenario), skip unzip and just optionally delete gz
        try:
            head_tsv = self._head(dest_tsv_key)
            if int(head_tsv.get("ContentLength", 0)) > 0:
                if delete_gz_after_unzip:
                    self.s3_client.delete_object(Bucket=self.bucket, Key=dest_gz_key)
                return {
                    "selected_src_key": selected_src_key,
                    "gz_key": dest_gz_key,
                    "tsv_key": dest_tsv_key,
                    "bytes": str(head_tsv.get("ContentLength", 0)),
                    "sha256": "",
                }
        except Exception:
            pass

        resp = self.s3_client.get_object(Bucket=self.bucket, Key=dest_gz_key)
        reader = _GzipStreamingReader(resp["Body"])

        # Slow but reliable streaming upload
        self.s3_client.upload_fileobj(reader, self.bucket, dest_tsv_key)

        # Validate TSV upload size matches streamed bytes
        head = self._head(dest_tsv_key)
        dest_len = int(head.get("ContentLength", 0))
        if dest_len != int(reader.bytes_out):
            raise Exception(
                f"Unzip size mismatch for {dest_tsv_key}: uploaded={dest_len}, streamed={reader.bytes_out}"
            )

        if delete_gz_after_unzip:
            self.s3_client.delete_object(Bucket=self.bucket, Key=dest_gz_key)

        return {
            "selected_src_key": selected_src_key,
            "gz_key": dest_gz_key,
            "tsv_key": dest_tsv_key,
            "bytes": str(reader.bytes_out),
            "sha256": reader.sha256.hexdigest(),
        }





from airflow.decorators import task

@task(task_id="COPY_UNZIP_TO_DATASET_LANDING")
def ${method}(**context):
    from packages.get_batch_number import S3_Object_Management

    mgr = S3_Object_Management(${bucket_name})

    result = mgr.move_gz_to_dataset_and_unzip_oldest_first(
        source_location="${source_location}",
        target_location="${target_location}",
        file_prefix="${file_prefix}",
        delete_gz_after_unzip=True,
    )

    # Keep logs short
    print(f"Picked: {result['selected_src_key']}")
    print(f"TSV:    {result['tsv_key']}")
    print(f"Bytes:  {result['bytes']}")

    # returning TSV key is useful if any downstream wants it
    return result["tsv_key"]