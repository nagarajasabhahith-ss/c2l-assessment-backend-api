"""
Storage abstraction: local filesystem or Google Cloud Storage.

When GCS_BUCKET is set, uploads go to GCS and file_path is stored as gs://bucket/key.
When not set, files are stored under UPLOAD_DIR as before.
"""

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from app.config import settings

# Lazy GCS client to avoid import errors when GCS not used
_gcs_client = None


def _get_gcs_client():
    global _gcs_client
    if _gcs_client is not None:
        return _gcs_client
    if not settings.gcs_enabled:
        return None
    try:
        from google.cloud import storage
        if settings.GCS_CREDENTIALS_PATH:
            client = storage.Client.from_service_account_json(settings.GCS_CREDENTIALS_PATH)
        else:
            client = storage.Client()
        _gcs_client = client
        return client
    except Exception:
        return None


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, blob_name) from gs://bucket/prefix/key."""
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    path = uri[5:]  # strip gs://
    idx = path.find("/")
    if idx < 0:
        raise ValueError(f"Invalid GCS URI: {uri}")
    return path[:idx], path[idx + 1:]


def upload_file(
    assessment_id: str,
    file_id: str,
    ext: str,
    content: bytes,
) -> str:
    """
    Store file and return the stored path (local path or gs://...).
    Uses GCS when settings.gcs_enabled else local UPLOAD_DIR.
    """
    safe_filename = f"{file_id}{ext}"
    if settings.gcs_enabled:
        client = _get_gcs_client()
        if client is None:
            # Fallback to local if GCS client failed
            return _upload_local(assessment_id, safe_filename, content)
        bucket = client.bucket(settings.GCS_BUCKET)
        blob_name = f"{settings.GCS_PREFIX.strip('/')}/{assessment_id}/{safe_filename}"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content, content_type="application/octet-stream")
        return f"gs://{settings.GCS_BUCKET}/{blob_name}"
    return _upload_local(assessment_id, safe_filename, content)


def _upload_local(assessment_id: str, safe_filename: str, content: bytes) -> str:
    assessment_dir = Path(settings.UPLOAD_DIR) / str(assessment_id)
    assessment_dir.mkdir(parents=True, exist_ok=True)
    file_path = assessment_dir / safe_filename
    file_path.write_bytes(content)
    return str(file_path)


def delete_file(path: str) -> None:
    """Delete file at path (local file or GCS object)."""
    if path.startswith("gs://"):
        try:
            client = _get_gcs_client()
            if client is not None:
                bucket_name, blob_name = _parse_gcs_uri(path)
                bucket = client.bucket(bucket_name)
                bucket.blob(blob_name).delete()
        except Exception as e:
            # Log but don't fail the request
            import logging
            logging.getLogger(__name__).warning("GCS delete failed for %s: %s", path, e)
    else:
        try:
            if os.path.isfile(path):
                os.remove(path)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("Local delete failed for %s: %s", path, e)


@contextmanager
def get_local_path(path: str):
    """
    Yield a local file path for reading. If path is gs://..., download to a temp file
    and yield that; the temp file is removed on exit.
    Raises FileNotFoundError if path is local and the file does not exist.
    """
    if not path.startswith("gs://"):
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        yield path
        return
    client = _get_gcs_client()
    if client is None:
        raise RuntimeError("GCS path given but GCS client not available")
    bucket_name, blob_name = _parse_gcs_uri(path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    suffix = Path(blob_name).suffix or ""
    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    try:
        os.close(fd)
        blob.download_to_filename(tmp_path)
        yield tmp_path
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
