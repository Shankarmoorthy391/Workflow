"""S3 storage for job automation document uploads."""

import os
import re
from datetime import date
from typing import Optional, Tuple
from urllib.parse import urlparse

import boto3

USE_S3 = os.getenv("USE_S3", "False").lower() in ("true", "1", "yes")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_STORAGE_BUCKET_NAME = os.getenv("AWS_STORAGE_BUCKET_NAME", "")
AWS_S3_CUSTOM_DOMAIN = os.getenv("AWS_S3_CUSTOM_DOMAIN", "").rstrip("/")
AWS_S3_REGION_NAME = os.getenv("AWS_S3_REGION_NAME", "ap-south-1")
AWS_LOCATION = os.getenv("AWS_LOCATION", "media")
DOCUMENT_PREFIX = "job_automation_documents"


def _sanitize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()


def job_automation_document_upload_path(
    filename: str,
    database_name: str,
    schema_name: str = "public",
) -> str:
    """
    S3 object key:
      media/job_automation_documents/{database}/{schema}/YYYY/MM/DD/filename.ext
    """
    database_name = _sanitize_name(database_name or "default")
    schema_name = _sanitize_name(schema_name or "public")
    today = date.today()
    rel_path = (
        f"{DOCUMENT_PREFIX}/{database_name}/{schema_name}/"
        f"{today:%Y/%m/%d}/{filename}"
    )
    location = AWS_LOCATION.strip("/")
    return f"{location}/{rel_path}" if location else rel_path


def _domain_with_scheme() -> str:
    domain = AWS_S3_CUSTOM_DOMAIN.rstrip("/")
    if not domain:
        return ""
    if not domain.startswith("http://") and not domain.startswith("https://"):
        domain = f"https://{domain}"
    return domain


def build_public_url(s3_key: str) -> str:
    domain = _domain_with_scheme()
    if domain:
        return f"{domain}/{s3_key.lstrip('/')}"
    return (
        f"https://{AWS_STORAGE_BUCKET_NAME}.s3.{AWS_S3_REGION_NAME}.amazonaws.com/"
        f"{s3_key.lstrip('/')}"
    )


def normalize_file_url(stored_path: Optional[str]) -> Optional[str]:
    """Return a full https URL from a stored file_path value."""
    if not stored_path:
        return None
    fp = stored_path.strip()
    if fp.startswith("http://") or fp.startswith("https://"):
        return fp

    domain = _domain_with_scheme()
    if not domain:
        return None

    bare_domain = domain.replace("https://", "").replace("http://", "").rstrip("/")
    if fp.startswith(f"{bare_domain}/"):
        return f"https://{fp}"
    if fp.startswith("media/"):
        return f"{domain}/{fp.lstrip('/')}"
    return None


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_S3_REGION_NAME,
    )


def upload_bytes(
    content: bytes,
    filename: str,
    database_name: str,
    schema_name: str = "public",
    content_type: str = "application/pdf",
) -> Tuple[str, str]:
    """Upload bytes to S3. Returns (s3_key, public_url)."""
    s3_key = job_automation_document_upload_path(filename, database_name, schema_name)
    _s3_client().put_object(
        Bucket=AWS_STORAGE_BUCKET_NAME,
        Key=s3_key,
        Body=content,
        ContentType=content_type,
    )
    return s3_key, build_public_url(s3_key)


def delete_object(s3_key: str) -> None:
    _s3_client().delete_object(Bucket=AWS_STORAGE_BUCKET_NAME, Key=s3_key)


def s3_key_from_stored_path(stored_path: str) -> Optional[str]:
    if not stored_path:
        return None
    if stored_path.startswith("http://") or stored_path.startswith("https://"):
        return urlparse(stored_path).path.lstrip("/")

    bare_domain = AWS_S3_CUSTOM_DOMAIN.replace("https://", "").replace("http://", "").rstrip("/")
    if bare_domain and stored_path.startswith(f"{bare_domain}/"):
        return stored_path[len(bare_domain) + 1:]

    if stored_path.startswith("media/"):
        return stored_path.lstrip("/")

    if stored_path.startswith("uploads/") or stored_path.startswith("uploads\\"):
        return None
    return stored_path.lstrip("/")


def is_remote_path(stored_path: str) -> bool:
    return bool(normalize_file_url(stored_path))


def is_s3_stored_path(stored_path: str) -> bool:
    return is_remote_path(stored_path)
