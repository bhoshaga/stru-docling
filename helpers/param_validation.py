"""
Parameter validation for API endpoints.

Validates enum-like parameters upfront to return helpful 422 errors
instead of accepting invalid values and failing later at worker level.
"""
import platform
from typing import Optional

IS_MACOS = platform.system() == "Darwin"

# Valid values for enum-like parameters
VALID_PIPELINES = ["standard", "vlm", "asr"]  # "legacy" not supported by controller
VALID_IMAGE_EXPORT_MODES = ["embedded", "placeholder", "referenced"]
VALID_TABLE_MODES = ["fast", "accurate"]
VALID_OCR_ENGINES = ["auto", "easyocr", "rapidocr", "tesserocr", "tesseract"]
if IS_MACOS:
    VALID_OCR_ENGINES.append("ocrmac")
VALID_VLM_MODELS = [
    # Only granite_docling is currently supported (model downloaded in prepare.sh)
    "granite_docling",
    # "smoldocling",
    # "smoldocling_vllm",
    # "granite_vision",
    # "granite_vision_vllm",
    # "granite_vision_ollama",
    # "got_ocr_2",
    # "granite_docling_vllm",
]
VALID_ASR_MODELS = [
    # Only whisper_turbo is supported in production (auto-detects MLX on Mac, native/CUDA on Linux)
    "whisper_turbo",
    # Other models are supported by docling but not pre-downloaded:
    # "whisper_tiny",
    # "whisper_small",
    # "whisper_medium",
    # "whisper_base",
    # "whisper_large",
    # Explicit MLX models (Apple Silicon only):
    # "whisper_tiny_mlx", "whisper_small_mlx", "whisper_medium_mlx",
    # "whisper_base_mlx", "whisper_large_mlx", "whisper_turbo_mlx",
    # Explicit Native models (native/CUDA):
    # "whisper_tiny_native", "whisper_small_native", "whisper_medium_native",
    # "whisper_base_native", "whisper_large_native", "whisper_turbo_native",
]


def validate_conversion_params(
    pipeline: str = "standard",
    image_export_mode: str = "embedded",
    table_mode: str = "fast",
    ocr_engine: str = "easyocr",
    vlm_pipeline_model: Optional[str] = None,
    asr_pipeline_model: Optional[str] = None,
) -> Optional[str]:
    """
    Validate all conversion parameters at once.

    Returns error message if any parameter is invalid, None if all OK.

    Args:
        pipeline: Processing pipeline (legacy, standard, vlm, asr)
        image_export_mode: Image export mode (embedded, placeholder, referenced)
        table_mode: Table structure mode (fast, accurate)
        ocr_engine: OCR engine to use
        vlm_pipeline_model: VLM model preset for vlm pipeline
        asr_pipeline_model: ASR model preset for asr pipeline (whisper_turbo, etc.)

    Returns:
        Error message string if validation fails, None if all parameters are valid.
    """
    # Validate pipeline
    if pipeline not in VALID_PIPELINES:
        return f"Invalid pipeline '{pipeline}'. Valid options: {', '.join(VALID_PIPELINES)}"

    # Validate image_export_mode
    if image_export_mode not in VALID_IMAGE_EXPORT_MODES:
        return f"Invalid image_export_mode '{image_export_mode}'. Valid options: {', '.join(VALID_IMAGE_EXPORT_MODES)}"

    # Validate table_mode
    if table_mode not in VALID_TABLE_MODES:
        return f"Invalid table_mode '{table_mode}'. Valid options: {', '.join(VALID_TABLE_MODES)}"

    # Validate ocr_engine
    if ocr_engine == "ocrmac" and not IS_MACOS:
        return "ocrmac is only available on macOS. Valid options: " + ", ".join(VALID_OCR_ENGINES)
    if ocr_engine not in VALID_OCR_ENGINES:
        return f"Invalid ocr_engine '{ocr_engine}'. Valid options: {', '.join(VALID_OCR_ENGINES)}"

    # Validate vlm_pipeline_model (only if provided)
    if vlm_pipeline_model is not None and vlm_pipeline_model not in VALID_VLM_MODELS:
        return f"Invalid vlm_pipeline_model '{vlm_pipeline_model}'. Valid options: {', '.join(VALID_VLM_MODELS)}"

    # Validate asr_pipeline_model (only if provided)
    if asr_pipeline_model is not None and asr_pipeline_model not in VALID_ASR_MODELS:
        return f"Invalid asr_pipeline_model '{asr_pipeline_model}'. Valid options: {', '.join(VALID_ASR_MODELS)}"

    return None


def validate_chunk_params(body: dict) -> Optional[str]:
    """
    Validate chunk endpoint parameters (convert_* prefixed).

    Used by chunk endpoints which accept parameters with 'convert_' prefix.

    Args:
        body: JSON body dict from request

    Returns:
        Error message string if validation fails, None if all parameters are valid.
    """
    # Extract convert_* params with defaults
    pipeline = body.get("convert_pipeline", "standard")
    image_export_mode = body.get("convert_image_export_mode", "embedded")
    table_mode = body.get("convert_table_mode", "fast")
    ocr_engine = body.get("convert_ocr_engine", "easyocr")
    vlm_pipeline_model = body.get("convert_vlm_pipeline_model")
    asr_pipeline_model = body.get("convert_asr_pipeline_model")

    # Validate pipeline
    if pipeline not in VALID_PIPELINES:
        return f"Invalid convert_pipeline '{pipeline}'. Valid options: {', '.join(VALID_PIPELINES)}"

    # Validate image_export_mode
    if image_export_mode not in VALID_IMAGE_EXPORT_MODES:
        return f"Invalid convert_image_export_mode '{image_export_mode}'. Valid options: {', '.join(VALID_IMAGE_EXPORT_MODES)}"

    # Validate table_mode
    if table_mode not in VALID_TABLE_MODES:
        return f"Invalid convert_table_mode '{table_mode}'. Valid options: {', '.join(VALID_TABLE_MODES)}"

    # Validate ocr_engine
    if ocr_engine == "ocrmac" and not IS_MACOS:
        return "ocrmac is only available on macOS. Valid options: " + ", ".join(VALID_OCR_ENGINES)
    if ocr_engine not in VALID_OCR_ENGINES:
        return f"Invalid convert_ocr_engine '{ocr_engine}'. Valid options: {', '.join(VALID_OCR_ENGINES)}"

    # Validate vlm_pipeline_model (only if provided)
    if vlm_pipeline_model is not None and vlm_pipeline_model not in VALID_VLM_MODELS:
        return f"Invalid convert_vlm_pipeline_model '{vlm_pipeline_model}'. Valid options: {', '.join(VALID_VLM_MODELS)}"

    # Validate asr_pipeline_model (only if provided)
    if asr_pipeline_model is not None and asr_pipeline_model not in VALID_ASR_MODELS:
        return f"Invalid convert_asr_pipeline_model '{asr_pipeline_model}'. Valid options: {', '.join(VALID_ASR_MODELS)}"

    return None
