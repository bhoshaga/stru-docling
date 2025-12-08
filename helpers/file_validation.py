"""
File type validation for docling-serve supported formats.
"""
from typing import Optional, Set

# Supported file extensions (lowercase, without dot)
# Based on: https://docling-project.github.io/docling/usage/supported_formats/
SUPPORTED_EXTENSIONS: Set[str] = {
    # Documents
    "pdf",
    "docx",
    "pptx",
    "xlsx",
    # Markup
    "html",
    "htm",
    "md",
    "markdown",
    "adoc",
    "asciidoc",
    # Data
    "csv",
    "xml",
    "json",
    # Images
    "png",
    "jpg",
    "jpeg",
    "tiff",
    "tif",
    "bmp",
    "gif",
    "webp",
    # Audio
    "wav",
    "mp3",
    # Subtitles
    "vtt",
}


def get_file_extension(filename: str) -> str:
    """Extract lowercase extension from filename."""
    if "." not in filename:
        return ""
    return filename.rsplit(".", 1)[-1].lower()


def is_supported_file(filename: str) -> bool:
    """Check if file type is supported by docling-serve."""
    ext = get_file_extension(filename)
    return ext in SUPPORTED_EXTENSIONS


def validate_file_type(filename: str) -> Optional[str]:
    """
    Validate file type is supported.

    Returns:
        None if valid, error message if invalid.
    """
    ext = get_file_extension(filename)

    if not ext:
        return f"File '{filename}' has no extension. Supported formats: PDF, DOCX, PPTX, XLSX, HTML, MD, images (PNG, JPG, TIFF), audio (WAV, MP3)"

    if ext not in SUPPORTED_EXTENSIONS:
        return f"File type '.{ext}' is not supported. Supported formats: PDF, DOCX, PPTX, XLSX, HTML, MD, images (PNG, JPG, TIFF), audio (WAV, MP3)"

    return None
