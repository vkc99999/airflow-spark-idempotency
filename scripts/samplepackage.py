import io
import re
import gzip
import hashlib
from typing import Dict, List, Optional, Tuple


class _GzipStreamingReader(io.RawIOBase):
    """
    File-like object that streams uncompressed bytes from an S3 Body stream.
    upload_fileobj() will pull from read() until EOF.
    Also tracks bytes + sha256 while streaming.
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
    # ... keep your existing __init__, archive_file, validate_copy, etc.

    _TSV_GZ_RE = re.compile(
        r"^(?P<prefix>.+)-(?P<ts>\d{14})-(?P<kind>Full|Incremental)-(?P<part>\d+)\.tsv\.gz$"
    )

    def _list_keys_under_prefix(self, prefix: str) -> List[str]:
        prefix = prefix.lstrip("/")
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

    def _parse_gz_name(
        self,
        filename: str,
        expected_file_prefix: str,
    ) -> Optional[Tuple[str, int]]:
        """
        Returns (ts, part) if filename matches expected prefix and pattern.
        """
        m = self._TSV_GZ_RE.match(filename)
        if not m:
            return None
        if m.group("prefix") != expected_file_prefix:
            return None
        ts = m.group("ts")
        part = int(m.group("part"))
        return ts, part

    def _pick_next_fifo_gz_key(
        self,
        source_location: str,
        file_prefix: str,
    ) -> Optional[str]:
        """
        FIFO pick:
          - oldest timestamp first
          - within same timestamp, smallest part number
          - do not process part>1 unless part1 exists for that timestamp
        Returns the S3 key to process next, or None if nothing eligible yet.
        """
        src_prefix = source_location.strip("/")
        if not src_prefix.endswith("/"):
            src_prefix += "/"

        all_keys = self._list_keys_under_prefix(src_prefix)

        candidates: List[Tuple[str, int, str]] = []
        for k in all_keys:
            fn = k.split("/")[-1]
            if not fn.endswith(".tsv.gz"):
                continue
            meta = self._parse_gz_name(fn, file_prefix)
            if not meta:
                continue
            ts, part = meta
            candidates.append((ts, part, k))

        if not candidates:
            return None

        # Find oldest timestamp present
        oldest_ts = min(t[0] for t in candidates)

        # Consider only files from oldest timestamp
        oldest = [(ts, part, k) for (ts, part, k) in candidates if ts == oldest_ts]
        parts_present = {part for (_, part, _) in oldest}

        # Gate: part 1 must exist
        if 1 not in parts_present:
            return None

        # Pick the smallest available part (1, then 2, then 3...)
        ts, part, key = sorted(oldest, key=lambda x: x[1])[0]
        return key

    def move_gz_to_dataset_and_unzip_fifo(
        self,
        source_location: str,
        target_location: str,
        file_prefix: str,
        delete_gz_after_unzip: bool = True,
    ) -> Dict[str, str]:
        """
        Picks next gz (FIFO), copies it to target_location, unzips to .tsv in target_location.
        Returns {"gz_key": ..., "tsv_key": ..., "sha256": ..., "bytes": "..."}
        """
        next_src_key = self._pick_next_fifo_gz_key(source_location, file_prefix)
        if not next_src_key:
            raise Exception(
                f"No eligible .tsv.gz found yet under {source_location} for prefix={file_prefix} "
                f"(waiting for part-1 or files)."
            )

        filename_gz = next_src_key.split("/")[-1]
        if not filename_gz.endswith(".gz"):
            raise Exception(f"Unexpected file (not .gz): {filename_gz}")

        # 1) Copy gz from current -> dataset landing (and delete from current)
        # Reuse your existing validated copy+delete logic.
        gz_key_in_dataset = self.archive_file(next_src_key, target_location)

        # 2) Unzip gz (in dataset) -> tsv (same folder)
        tsv_key_in_dataset = gz_key_in_dataset[:-3]  # drop ".gz"

        # If TSV already exists (retry case), skip unzip and just clean gz if needed
        try:
            head_tsv = self.s3_client.head_object(Bucket=self.bucket, Key=tsv_key_in_dataset)
            if head_tsv.get("ContentLength", 0) > 0:
                if delete_gz_after_unzip:
                    self.s3_client.delete_object(Bucket=self.bucket, Key=gz_key_in_dataset)
                return {
                    "gz_key": gz_key_in_dataset,
                    "tsv_key": tsv_key_in_dataset,
                    "sha256": "",
                    "bytes": str(head_tsv.get("ContentLength", 0)),
                }
        except Exception:
            pass

        resp = self.s3_client.get_object(Bucket=self.bucket, Key=gz_key_in_dataset)
        body_stream = resp["Body"]

        reader = _GzipStreamingReader(body_stream)

        # upload_fileobj streams and is slower but reliable (multipart when needed)
        self.s3_client.upload_fileobj(reader, self.bucket, tsv_key_in_dataset)

        # 3) Validate unzip upload: compare TSV ContentLength to bytes we streamed
        head = self.s3_client.head_object(Bucket=self.bucket, Key=tsv_key_in_dataset)
        dest_len = int(head.get("ContentLength", 0))
        if dest_len != reader.bytes_out:
            raise Exception(
                f"Unzip size mismatch for {tsv_key_in_dataset}: "
                f"uploaded={dest_len}, streamed={reader.bytes_out}"
            )

        if delete_gz_after_unzip:
            self.s3_client.delete_object(Bucket=self.bucket, Key=gz_key_in_dataset)

        return {
            "gz_key": gz_key_in_dataset,
            "tsv_key": tsv_key_in_dataset,
            "sha256": reader.sha256.hexdigest(),
            "bytes": str(reader.bytes_out),
        }
        
        
        
@task(task_id="copy_unzip_to_dataset_landing")
def ${method}(**context):
    from packages.get_batch_number import S3_Object_Management

    mgr = S3_Object_Management(${bucket_name})

    result = mgr.move_gz_to_dataset_and_unzip_fifo(
        source_location="${source_location}",
        target_location="${target_location}",
        file_prefix="${file_prefix}",
        delete_gz_after_unzip=True,
    )

    print(f"Selected gz: {result['gz_key']}")
    print(f"Created tsv:  {result['tsv_key']}")
    print(f"Bytes:       {result['bytes']}")
    print(f"SHA256:      {result['sha256']}")

    # Return TSV key so downstream tasks can use it if needed
    return result["tsv_key"]