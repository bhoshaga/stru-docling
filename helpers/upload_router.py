"""
Upload endpoint for file storage with deduplication.
"""
import hashlib
import logging
import os
import time
import uuid
from typing import Dict, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["upload"])

# Upload storage config (matches result_storage default ./data structure)
UPLOADS_DIR = os.environ.get("UPLOADS_DIR", "./data/uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

# Uploaded files tracking: {file_id: {"api_key": str, "filepath": str, "filename": str, "uploaded_at": float, "hash": str}}
uploaded_files: Dict[str, dict] = {}

# Hash to file_id mapping for deduplication: {sha256_hash: file_id}
hash_to_file_id: Dict[str, str] = {}

# Security
security = HTTPBearer()


def init_router(
    verify_api_key_func,
    check_file_limits_func,
):
    """
    Initialize the router with dependencies from the main controller.

    This avoids circular imports by injecting dependencies at runtime.
    Only needs auth and limit checking for upload endpoint.
    """
    global _verify_api_key, _check_file_limits

    _verify_api_key = verify_api_key_func
    _check_file_limits = check_file_limits_func


# =============================================================================
# UPLOAD ENDPOINT
# =============================================================================

@router.post("/v1/upload")
async def upload_file(
    files: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    Upload a file for later conversion.

    Useful for large files that exceed HTTP body limits.
    Returns a file_id that can be used with /v1/convert/source endpoints.

    Security: Files are stored with UUID names, not original filenames.
    """
    api_key = await _verify_api_key(credentials)

    file_bytes = await files.read()
    original_filename = files.filename or "document.pdf"
    is_pdf = original_filename.lower().endswith(".pdf")
    file_size = len(file_bytes)

    # Calculate SHA256 hash for deduplication
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    logger.info(f"[UPLOAD] Received file: {original_filename}, size={file_size} bytes, hash={file_hash}, api_key={api_key}")

    # Check if file already uploaded (same hash)
    if file_hash in hash_to_file_id:
        existing_file_id = hash_to_file_id[file_hash]
        existing_info = uploaded_files.get(existing_file_id)
        if existing_info and os.path.exists(existing_info["filepath"]):
            logger.info(f"[UPLOAD] Duplicate detected: hash={file_hash} already exists as file_id={existing_file_id}")
            return {
                "file_id": existing_file_id,
                "filename": original_filename,
                "size_bytes": file_size,
                "duplicate": True,
                "message": "File already uploaded. Returning existing file_id.",
            }
        else:
            # Hash exists but file is gone - clean up stale mapping
            del hash_to_file_id[file_hash]
            if existing_file_id in uploaded_files:
                del uploaded_files[existing_file_id]

    # Check tier limits and file type
    limit_error = _check_file_limits(file_bytes, api_key, filename=original_filename, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Generate UUID-based filename for security (don't use original filename)
    file_id = str(uuid.uuid4())
    # Keep extension for content-type detection, but use UUID as filename
    ext = os.path.splitext(original_filename)[1].lower() or ".bin"
    # Sanitize extension (only allow known safe extensions)
    safe_extensions = {".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls", ".html", ".txt", ".md", ".png", ".jpg", ".jpeg"}
    if ext not in safe_extensions:
        ext = ".bin"

    filepath = os.path.join(UPLOADS_DIR, f"{file_id}{ext}")

    with open(filepath, "wb") as f:
        f.write(file_bytes)

    # Track the upload (store original filename for reference, but file is saved with UUID)
    uploaded_files[file_id] = {
        "api_key": api_key,
        "filepath": filepath,
        "filename": original_filename,  # Original name for display/logging only
        "uploaded_at": time.time(),
        "size_bytes": file_size,
        "hash": file_hash,
    }

    # Register hash mapping for deduplication
    hash_to_file_id[file_hash] = file_id

    logger.info(f"[UPLOAD] Saved file as {file_id}{ext} (original: {original_filename}, hash={file_hash})")

    return {
        "file_id": file_id,
        "filename": original_filename,
        "size_bytes": file_size,
        "message": "File uploaded successfully. Use file_id with /v1/convert/source endpoints.",
    }


def cleanup_old_uploads(max_age_seconds: int = 3600) -> int:
    """Remove uploaded files older than max_age_seconds."""
    now = time.time()
    to_remove = []

    for file_id, info in uploaded_files.items():
        age = now - info["uploaded_at"]
        if age > max_age_seconds:
            to_remove.append(file_id)

    for file_id in to_remove:
        info = uploaded_files.pop(file_id, None)
        if info:
            # Clean up hash mapping
            file_hash = info.get("hash")
            if file_hash and file_hash in hash_to_file_id:
                del hash_to_file_id[file_hash]

            # Delete file from disk
            if os.path.exists(info["filepath"]):
                try:
                    os.remove(info["filepath"])
                    logger.info(f"[UPLOAD] Cleaned up old file: {file_id} (hash={file_hash if file_hash else 'N/A'})")
                except Exception as e:
                    logger.warning(f"[UPLOAD] Failed to delete file {file_id}: {e}")

    return len(to_remove)
