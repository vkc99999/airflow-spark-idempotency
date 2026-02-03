import gzip
import hashlib
import re
from typing import Dict, List, Optional, Tuple


class S3_Object_Management:
    # keep your existing __init__ etc. Just ensure you have:
    # self.bucket and self.s3_client

    def _norm_prefix(self, prefix: str) -> str:
        """Normalize S3 prefix to 'a/b/c/' or ''."""
        p = (prefix or "").lstrip("/")
        if p and not p.endswith("/"):
            p += "/"
        return p

    def _list_keys_under_prefix(self, prefix: str) -> List[str]:
        """List all keys under prefix (handles pagination)."""
        prefix = self._norm_prefix(prefix)
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
        """Head object for size validation."""
        return self.s3_client.head_object(Bucket=self.bucket, Key=key)

    def _copy_validate_delete(self, source_key: str, dest_key: str) -> None:
        """Copy within bucket, validate size, then delete source."""
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
        """Parse '<prefix>-<14ts>-<token>-<part>.tsv.gz' -> (ts, part)."""
        rx = re.compile(rf"^{re.escape(file_prefix)}-(\d{{14}})-[^-]+-(\d+)\.tsv\.gz$")
        m = rx.match(filename)
        if not m:
            return None
        return m.group(1), int(m.group(2))

    def _pick_oldest_ts_then_smallest_part(self, keys: List[str], file_prefix: str) -> str:
        """Pick oldest timestamp; within it pick smallest part."""
        candidates: List[Tuple[str, int, str]] = []
        for k in keys:
            fn = k.split("/")[-1]
            if not fn.endswith(".tsv.gz"):
                continue
            meta = self._parse_ts_part(fn, file_prefix)
            if not meta:
                continue
            ts, part = meta
            candidates.append((ts, part, k))

        if not candidates:
            raise Exception(f"No matching .tsv.gz keys for prefix={file_prefix}")

        oldest_ts = min(ts for (ts, _, _) in candidates)
        group = [(part, key) for (ts, part, key) in candidates if ts == oldest_ts]
        _, selected_key = min(group, key=lambda x: x[0])
        return selected_key

    def _gunzip_s3_to_s3_multipart(
        self,
        gz_key: str,
        tsv_key: str,
        part_size_bytes: int = 64 * 1024 * 1024,
    ) -> Dict[str, str]:
        """Stream gunzip gz_key -> tsv_key using multipart upload + size validation."""
        if part_size_bytes < 5 * 1024 * 1024:
            raise ValueError("part_size_bytes must be >= 5MB.")

        resp = self.s3_client.get_object(Bucket=self.bucket, Key=gz_key)
        gz = gzip.GzipFile(fileobj=resp["Body"])

        sha = hashlib.sha256()
        bytes_out = 0

        mp = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=tsv_key)
        upload_id = mp["UploadId"]
        parts = []

        def _abort():
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket, Key=tsv_key, UploadId=upload_id
                )
            except Exception:
                pass

        try:
            part_number = 1
            buf = bytearray()

            while True:
                chunk = gz.read(part_size_bytes)
                if not chunk:
                    break

                sha.update(chunk)
                bytes_out += len(chunk)
                buf.extend(chunk)

                while len(buf) >= part_size_bytes:
                    body = bytes(buf[:part_size_bytes])
                    del buf[:part_size_bytes]

                    up = self.s3_client.upload_part(
                        Bucket=self.bucket,
                        Key=tsv_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=body,
                    )
                    parts.append({"ETag": up["ETag"], "PartNumber": part_number})
                    part_number += 1

            if buf or part_number == 1:
                body = bytes(buf)

                if part_number == 1 and len(body) <= 5 * 1024 * 1024:
                    _abort()
                    self.s3_client.put_object(Bucket=self.bucket, Key=tsv_key, Body=body)
                else:
                    up = self.s3_client.upload_part(
                        Bucket=self.bucket,
                        Key=tsv_key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=body,
                    )
                    parts.append({"ETag": up["ETag"], "PartNumber": part_number})

                    self.s3_client.complete_multipart_upload(
                        Bucket=self.bucket,
                        Key=tsv_key,
                        UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )
            else:
                _abort()
                self.s3_client.put_object(Bucket=self.bucket, Key=tsv_key, Body=b"")

        except Exception:
            _abort()
            raise

        head = self._head(tsv_key)
        if int(head.get("ContentLength", 0)) != int(bytes_out):
            raise Exception(
                f"Unzip size mismatch for {tsv_key}: "
                f"uploaded={head.get('ContentLength')}, streamed={bytes_out}"
            )

        return {"bytes": str(bytes_out), "sha256": sha.hexdigest()}

    def move_gz_to_dataset_and_unzip_oldest_first(
        self,
        source_location: str,
        target_location: str,
        file_prefix: str,
        delete_gz_after_unzip: bool = True,
        part_size_bytes: int = 64 * 1024 * 1024,
    ) -> Dict[str, str]:
        """Pick next gz (oldest ts, smallest part), move to dataset, unzip, validate."""
        src_prefix = self._norm_prefix(source_location)
        tgt_prefix = self._norm_prefix(target_location)

        all_keys = self._list_keys_under_prefix(src_prefix)
        selected_src_key = self._pick_oldest_ts_then_smallest_part(all_keys, file_prefix)

        filename_gz = selected_src_key.split("/")[-1]
        dest_gz_key = f"{tgt_prefix}{filename_gz}"
        dest_tsv_key = dest_gz_key[:-3]

        # Retry-safe: if TSV already exists, don’t unzip again.
        try:
            h = self._head(dest_tsv_key)
            if int(h.get("ContentLength", 0)) > 0:
                try:
                    self._head(selected_src_key)
                    self._copy_validate_delete(selected_src_key, dest_gz_key)
                except Exception:
                    pass

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

        self._copy_validate_delete(selected_src_key, dest_gz_key)

        unzip_meta = self._gunzip_s3_to_s3_multipart(
            gz_key=dest_gz_key,
            tsv_key=dest_tsv_key,
            part_size_bytes=part_size_bytes,
        )

        if delete_gz_after_unzip:
            self.s3_client.delete_object(Bucket=self.bucket, Key=dest_gz_key)

        return {
            "selected_src_key": selected_src_key,
            "gz_key": dest_gz_key,
            "tsv_key": dest_tsv_key,
            "bytes": unzip_meta["bytes"],
            "sha256": unzip_meta["sha256"],
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

    print(result["tsv_key"])
    return result["tsv_key"]