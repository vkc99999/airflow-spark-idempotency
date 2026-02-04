import boto3
import gzip
import hashlib
import os
import re
from typing import Dict, List, Optional, Tuple, Any


class S3_Object_Management:
    def __init__(self, bucket: str):
        """S3 utility wrapper."""
        self.bucket = bucket
        self.s3_client = boto3.client("s3")

    def validate_copy(self, source_key: str, destination_key: str) -> Optional[dict]:
        """Validate copy using head_object size (ETag may differ for multipart)."""
        try:
            source_response = self.s3_client.head_object(Bucket=self.bucket, Key=source_key)
            destination_response = self.s3_client.head_object(Bucket=self.bucket, Key=destination_key)

            source_metadata = {
                "Size": source_response.get("ContentLength", 0),
                "ETag": source_response.get("ETag", "").strip('"'),
            }
            destination_metadata = {
                "Size": destination_response.get("ContentLength", 0),
                "ETag": destination_response.get("ETag", "").strip('"'),
            }

            if int(source_metadata.get("Size", 0)) != int(destination_metadata.get("Size", -1)):
                print(f" Size mismatch: Source={source_metadata.get('Size')}, Dest={destination_metadata.get('Size')}")
                return None

            return destination_metadata

        except Exception as e:
            print(f" Error validating copy: {str(e)}")
            return None

    def list_s3_objects_wildcard(self, prefix: str, filename_pattern: str) -> List[str]:
        s3_client = boto3.client("s3")
        regex_pattern = filename_pattern.replace("*", ".+").replace("?", ".") + "$"

        matching_keys = []
        continuation_token = None

        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
            if continuation_token:
                params["ContinuationToken"] = continuation_token

            response = s3_client.list_objects_v2(**params)
            if "Contents" not in response:
                break

            for obj in response["Contents"]:
                key = obj["Key"]
                filename = key[len(prefix):] if key.startswith(prefix) else key.split("/")[-1]
                if re.match(regex_pattern, filename):
                    matching_keys.append(key)

            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")

        return matching_keys

    def _norm_prefix(self, prefix: str) -> str:
        """Normalize S3 prefix to 'a/b/c/' or ''."""
        p = (prefix or "").lstrip("/")
        if p and not p.endswith("/"):
            p += "/"
        return p

    def _head(self, key: str) -> dict:
        """Head object."""
        return self.s3_client.head_object(Bucket=self.bucket, Key=key)

    def _parse_ts_token_part(self, filename: str, file_prefix: str) -> Optional[Tuple[str, str, int]]:
        """Parse '<prefix>-<14ts>-Full|Incremental-<part>.tsv.gz'."""
        rx = re.compile(
            rf"^{re.escape(file_prefix)}-(\d{{14}})-(Full|Incremental)-(\d+)\.tsv\.gz$",
            re.IGNORECASE,
        )
        m = rx.match(filename)
        if not m:
            return None
        ts = m.group(1)
        token = m.group(2).lower()  # full / incremental
        part = int(m.group(3))
        return ts, token, part

    def _pick_oldest_ts_then_full_then_smallest_part(self, keys: List[str], file_prefix: str) -> str:
        """FIFO by timestamp; prefer Full; then smallest part."""
        candidates: List[Tuple[str, int, int, str]] = []
        for k in keys:
            fn = k.split("/")[-1]
            if not fn.lower().endswith(".tsv.gz"):
                continue
            meta = self._parse_ts_token_part(fn, file_prefix)
            if not meta:
                continue
            ts, token, part = meta
            token_pri = 0 if token == "full" else 1
            candidates.append((ts, token_pri, part, k))

        if not candidates:
            raise Exception(f"No matching .tsv.gz keys for prefix={file_prefix}")

        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        return candidates[0][3]

    def _copy_to_dest_and_validate(self, source_key: str, dest_key: str) -> None:
        """Copy within bucket + validate size."""
        self.s3_client.copy_object(
            CopySource={"Bucket": self.bucket, "Key": source_key},
            Bucket=self.bucket,
            Key=dest_key,
        )
        if self.validate_copy(source_key, dest_key) is None:
            raise Exception(f"Copy validation failed: {source_key} -> {dest_key}")

    def _gunzip_s3_to_s3_multipart(
        self,
        gz_key: str,
        tsv_key: str,
        part_size_bytes: int = 64 * 1024 * 1024,
    ) -> Dict[str, Any]:
        """Stream gunzip gz_key -> tsv_key using multipart upload + size validation."""
        if part_size_bytes < 5 * 1024 * 1024:
            raise ValueError("part_size_bytes must be >= 5MB")

        resp = self.s3_client.get_object(Bucket=self.bucket, Key=gz_key)
        gz_stream = gzip.GzipFile(fileobj=resp["Body"])

        sha = hashlib.sha256()
        bytes_out = 0

        mp = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=tsv_key)
        upload_id = mp["UploadId"]
        parts = []
        part_number = 1

        def _abort():
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket, Key=tsv_key, UploadId=upload_id
                )
            except Exception:
                pass

        try:
            while True:
                chunk = gz_stream.read(part_size_bytes)
                if not chunk:
                    break

                sha.update(chunk)
                bytes_out += len(chunk)

                up = self.s3_client.upload_part(
                    Bucket=self.bucket,
                    Key=tsv_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"ETag": up["ETag"], "PartNumber": part_number})
                part_number += 1

            if bytes_out == 0:
                raise Exception("Unzipped output is empty")

            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=tsv_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            head = self.s3_client.head_object(Bucket=self.bucket, Key=tsv_key)
            if int(head.get("ContentLength", -1)) != int(bytes_out):
                raise Exception(
                    f"Unzip size mismatch: expected={bytes_out}, actual={head.get('ContentLength')}"
                )

            return {"bytes": bytes_out, "sha256": sha.hexdigest()}

        except Exception:
            _abort()
            raise

    def move_gz_to_dataset_and_unzip_oldest_first(
        self,
        source_location: str,
        target_location: str,
        file_prefix: str,
        delete_gz_after_unzip: bool = True,
        part_size_bytes: int = 64 * 1024 * 1024,
    ) -> Dict[str, str]:
        """Pick next file FIFO, copy to dataset landing, unzip to .tsv, return the .tsv key."""
        src_prefix = self._norm_prefix(source_location)
        tgt_prefix = self._norm_prefix(target_location)

        # Your wildcard uses '.+' for '*', so:
        # - pattern without leading '*' matches root files
        # - pattern with leading '*' matches nested paths but won't match root files
        keys_root = self.list_s3_objects_wildcard(src_prefix, f"{file_prefix}-*.tsv.gz")
        keys_nested = self.list_s3_objects_wildcard(src_prefix, f"*{file_prefix}-*.tsv.gz")
        all_keys = list(set(keys_root + keys_nested))

        selected_src_key = self._pick_oldest_ts_then_full_then_smallest_part(all_keys, file_prefix)

        filename_gz = selected_src_key.split("/")[-1]
        dest_gz_key = os.path.join(tgt_prefix, filename_gz).replace("\\", "/")
        dest_tsv_key = dest_gz_key[:-3]  # drop ".gz"

        # If TSV already exists (retry-safe), just return it.
        try:
            h = self._head(dest_tsv_key)
            if int(h.get("ContentLength", 0)) > 0:
                if delete_gz_after_unzip:
                    try:
                        self.s3_client.delete_object(Bucket=self.bucket, Key=dest_gz_key)
                    except Exception:
                        pass
                return {
                    "selected_src_key": selected_src_key,
                    "gz_key": dest_gz_key,
                    "tsv_key": dest_tsv_key,
                    "bytes": str(h.get("ContentLength", 0)),
                    "sha256": "",
                }
        except Exception:
            pass

        # Copy gz to dataset landing + validate
        self._copy_to_dest_and_validate(selected_src_key, dest_gz_key)

        # Unzip in dataset landing + validate output size
        unzip_meta = self._gunzip_s3_to_s3_multipart(
            gz_key=dest_gz_key,
            tsv_key=dest_tsv_key,
            part_size_bytes=part_size_bytes,
        )

        # Only after successful unzip, delete the source gz (so retries don’t skip ahead)
        self.s3_client.delete_object(Bucket=self.bucket, Key=selected_src_key)

        if delete_gz_after_unzip:
            self.s3_client.delete_object(Bucket=self.bucket, Key=dest_gz_key)

        return {
            "selected_src_key": selected_src_key,
            "gz_key": dest_gz_key,
            "tsv_key": dest_tsv_key,
            "bytes": str(unzip_meta["bytes"]),
            "sha256": str(unzip_meta["sha256"]),
        }