import gzip
import hashlib
import io
import re
from typing import Dict, List, Optional, Tuple

import boto3


# -----------------------------
# S3 low-level helpers (no class)
# -----------------------------
def _norm_prefix(prefix: str) -> str:
    p = (prefix or "").lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p


def list_keys_under_prefix(s3, bucket: str, prefix: str) -> List[str]:
    prefix = _norm_prefix(prefix)
    keys: List[str] = []
    token = None

    while True:
        params = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            params["ContinuationToken"] = token

        resp = s3.list_objects_v2(**params)
        for obj in resp.get("Contents") or []:
            keys.append(obj["Key"])

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    return keys


def head_object(s3, bucket: str, key: str) -> dict:
    return s3.head_object(Bucket=bucket, Key=key)


def copy_validate_delete(s3, bucket: str, source_key: str, dest_key: str) -> None:
    src = head_object(s3, bucket, source_key)

    s3.copy_object(
        CopySource={"Bucket": bucket, "Key": source_key},
        Bucket=bucket,
        Key=dest_key,
    )

    dst = head_object(s3, bucket, dest_key)
    if int(src.get("ContentLength", -1)) != int(dst.get("ContentLength", -2)):
        raise Exception(
            f"Copy size mismatch: {source_key} -> {dest_key} "
            f"(src={src.get('ContentLength')}, dst={dst.get('ContentLength')})"
        )

    s3.delete_object(Bucket=bucket, Key=source_key)


# -----------------------------
# Filename parsing + picking (oldest ts, then smallest part)
# -----------------------------
def parse_ts_part(filename: str, file_prefix: str) -> Optional[Tuple[str, int]]:
    """
    Accepts:
      <prefix>-<14digitTs>-<anything>-<part>.tsv.gz
    Example:
      PricerDimension-20251205140420-Incremental-2.tsv.gz
    Returns: (ts, part)
    """
    rx = re.compile(
        rf"^{re.escape(file_prefix)}-(\d{{14}})-[^-]+-(\d+)\.tsv\.gz$"
    )
    m = rx.match(filename)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def pick_oldest_ts_then_smallest_part(keys: List[str], file_prefix: str) -> str:
    candidates: List[Tuple[str, int, str]] = []
    for k in keys:
        fn = k.split("/")[-1]
        if not fn.endswith(".tsv.gz"):
            continue
        meta = parse_ts_part(fn, file_prefix)
        if not meta:
            continue
        ts, part = meta
        candidates.append((ts, part, k))

    if not candidates:
        raise Exception(f"No matching .tsv.gz keys for prefix={file_prefix}")

    oldest_ts = min(ts for (ts, _, _) in candidates)  # 14-digit => lex min works
    group = [(part, key) for (ts, part, key) in candidates if ts == oldest_ts]
    _, selected_key = min(group, key=lambda x: x[0])  # smallest part wins
    return selected_key


# -----------------------------
# Gunzip S3->S3 streaming (multipart upload, no temp files)
# -----------------------------
def gunzip_s3_to_s3_multipart(
    s3,
    bucket: str,
    gz_key: str,
    tsv_key: str,
    part_size_bytes: int = 64 * 1024 * 1024,  # 64MB
) -> Dict[str, str]:
    """
    Streams gz from S3, unzips, uploads TSV using multipart upload.
    Integrity:
      - gzip CRC is enforced by gzip while reading (throws if corrupt)
      - we track uncompressed bytes + sha256
      - we validate final TSV ContentLength == bytes_out
    """
    if part_size_bytes < 5 * 1024 * 1024:
        raise ValueError("part_size_bytes must be >= 5MB (S3 multipart minimum).")

    resp = s3.get_object(Bucket=bucket, Key=gz_key)
    gz_body = resp["Body"]  # StreamingBody

    # gzip CRC validation happens as we fully read the stream
    gz = gzip.GzipFile(fileobj=gz_body)

    sha = hashlib.sha256()
    bytes_out = 0

    # Start multipart upload
    mp = s3.create_multipart_upload(Bucket=bucket, Key=tsv_key)
    upload_id = mp["UploadId"]
    parts = []

    def _abort():
        try:
            s3.abort_multipart_upload(Bucket=bucket, Key=tsv_key, UploadId=upload_id)
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

            # Upload in >= part_size chunks (keeps memory bounded)
            while len(buf) >= part_size_bytes:
                body = bytes(buf[:part_size_bytes])
                del buf[:part_size_bytes]

                up = s3.upload_part(
                    Bucket=bucket,
                    Key=tsv_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=body,
                )
                parts.append({"ETag": up["ETag"], "PartNumber": part_number})
                part_number += 1

        # Last part (can be < 5MB)
        if buf or part_number == 1:
            body = bytes(buf)
            if part_number == 1 and len(body) <= 5 * 1024 * 1024:
                # Single small object: cancel multipart and do put_object
                _abort()
                s3.put_object(Bucket=bucket, Key=tsv_key, Body=body)
            else:
                up = s3.upload_part(
                    Bucket=bucket,
                    Key=tsv_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=body,
                )
                parts.append({"ETag": up["ETag"], "PartNumber": part_number})
                s3.complete_multipart_upload(
                    Bucket=bucket,
                    Key=tsv_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
        else:
            # No data produced (empty TSV)
            _abort()
            s3.put_object(Bucket=bucket, Key=tsv_key, Body=b"")

    except Exception:
        _abort()
        raise

    # Validate TSV size matches what we streamed out
    head = head_object(s3, bucket, tsv_key)
    if int(head.get("ContentLength", 0)) != int(bytes_out):
        raise Exception(
            f"Unzip size mismatch for {tsv_key}: "
            f"uploaded={head.get('ContentLength')}, streamed={bytes_out}"
        )

    return {"bytes": str(bytes_out), "sha256": sha.hexdigest()}


# -----------------------------
# Orchestrator you call from your existing class method
# -----------------------------
def move_gz_to_dataset_and_unzip_oldest_first(
    bucket: str,
    source_location: str,
    target_location: str,
    file_prefix: str,
    delete_gz_after_unzip: bool = True,
    part_size_bytes: int = 64 * 1024 * 1024,
) -> Dict[str, str]:
    """
    - lists all *.tsv.gz in source_location
    - picks oldest timestamp, then smallest part
    - moves ONLY that gz to target_location (copy+validate+delete)
    - unzips in target_location to .tsv (multipart streaming upload)
    - validates TSV size
    - optionally deletes the gz in target_location
    """
    s3 = boto3.client("s3")

    src_prefix = _norm_prefix(source_location)
    tgt_prefix = _norm_prefix(target_location)

    all_keys = list_keys_under_prefix(s3, bucket, src_prefix)
    selected_src_key = pick_oldest_ts_then_smallest_part(all_keys, file_prefix)

    filename_gz = selected_src_key.split("/")[-1]
    dest_gz_key = f"{tgt_prefix}{filename_gz}"
    dest_tsv_key = dest_gz_key[:-3]  # remove ".gz"

    # If TSV already exists (retry), don’t unzip again
    try:
        h = head_object(s3, bucket, dest_tsv_key)
        if int(h.get("ContentLength", 0)) > 0:
            # still move gz if it’s still in source
            try:
                head_object(s3, bucket, selected_src_key)
                copy_validate_delete(s3, bucket, selected_src_key, dest_gz_key)
            except Exception:
                pass

            if delete_gz_after_unzip:
                try:
                    s3.delete_object(Bucket=bucket, Key=dest_gz_key)
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

    # Move gz: source -> dataset
    copy_validate_delete(s3, bucket, selected_src_key, dest_gz_key)

    # Unzip: dataset gz -> dataset tsv
    unzip_meta = gunzip_s3_to_s3_multipart(
        s3=s3,
        bucket=bucket,
        gz_key=dest_gz_key,
        tsv_key=dest_tsv_key,
        part_size_bytes=part_size_bytes,
    )

    if delete_gz_after_unzip:
        s3.delete_object(Bucket=bucket, Key=dest_gz_key)

    return {
        "selected_src_key": selected_src_key,
        "gz_key": dest_gz_key,
        "tsv_key": dest_tsv_key,
        "bytes": unzip_meta["bytes"],
        "sha256": unzip_meta["sha256"],
    }
    
    
    
    
def move_gz_to_dataset_and_unzip_oldest_first(self, source_location, target_location, file_prefix):
    return move_gz_to_dataset_and_unzip_oldest_first(
        bucket=self.bucket,
        source_location=source_location,
        target_location=target_location,
        file_prefix=file_prefix,
        delete_gz_after_unzip=True,
    )
    