# Dockerfile for docling-serve with GIL-patched docling-parse
# Multi-worker setup with controller API v2
#
# BUILD WITH BUILDX (legacy docker build is deprecated):
#   docker buildx build --platform linux/amd64 -t docling-serve-patched:v7 .

FROM python:3.13-slim

# Install system dependencies + cloudflared for Cloudflare Tunnel + tesseract OCR
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    procps \
    lsof \
    curl \
    # Tesseract OCR with language packs (for tesseract/tesserocr engines)
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-fra \
    tesseract-ocr-deu \
    tesseract-ocr-spa \
    && curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
       -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install docling-serve and dependencies (includes stock docling-parse)
RUN pip install --no-cache-dir \
    docling-serve==1.9.0 \
    easyocr==1.7.2 \
    rapidocr-onnxruntime \
    onnxruntime \
    httpx \
    psutil \
    pypdf \
    pymupdf

# Copy prebuilt GIL-patched binary and overwrite stock version
COPY prebuilt/linux_x86_64_cp313/pdf_parsers.cpython-313-x86_64-linux-gnu.so /tmp/
RUN DOCLING_PARSE_PATH=$(python -c "import docling_parse; import os; print(os.path.dirname(docling_parse.__file__))") && \
    cp /tmp/pdf_parsers.cpython-313-x86_64-linux-gnu.so "$DOCLING_PARSE_PATH/" && \
    rm /tmp/pdf_parsers.cpython-313-x86_64-linux-gnu.so

# Pre-download HuggingFace models
RUN python -c "from docling.datamodel.pipeline_options import PipelineOptions; \
    from docling.document_converter import DocumentConverter; \
    DocumentConverter()"

# Pre-download EasyOCR models (detection + recognition for all default languages)
RUN python -c "import easyocr; reader = easyocr.Reader(['fr', 'de', 'es', 'en'], gpu=False)"

# Copy controller v2 with helpers
COPY controller.py /app/
COPY helpers/ /app/helpers/
COPY startup.sh /app/
COPY cert.pem /app/
RUN chmod +x /app/startup.sh

# Create data directory for logs and results
RUN mkdir -p /data

# Environment defaults
ENV DOCLING_DEVICE=cuda
ENV NUM_WORKERS=10
ENV DOCLING_SERVE_ENG_LOC_NUM_WORKERS=2
ENV DOCLING_DEBUG_PROFILE_PIPELINE_TIMINGS=true
ENV LOG_FILE=/app/server.log
ENV RESULT_STORAGE_DIR=/data

# Expose controller port
EXPOSE 8000

# Start multi-worker setup
CMD ["/app/startup.sh"]
