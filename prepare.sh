#!/bin/bash
#
# Prepare host for docling-serve deployment.
# Downloads models WITHOUT requiring torch/easyocr packages.
# Run this in parallel with docker pull for fastest startup.
#
# Usage:
#   ./prepare.sh [model_directory]
#   ./prepare.sh              # defaults to ~/models
#   ./prepare.sh /tmp/models  # custom path
#
# Then run container with:
#   docker run -v ~/models:/models ...

set -e

MODEL_DIR="${1:-$HOME/models}"
echo "=== Preparing models in: $MODEL_DIR ==="

# Create directories
mkdir -p "$MODEL_DIR/easyocr/model"
mkdir -p "$MODEL_DIR/huggingface"
mkdir -p "$MODEL_DIR/whisper"

# --- Install pip if missing ---
if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "[pip] Not found, installing..."
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3 - --break-system-packages --user 2>/dev/null || \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3 - --user
    export PATH="$HOME/.local/bin:$PATH"
fi

# Determine pip command
if command -v pip3 &> /dev/null; then
    PIP="pip3"
elif [ -f "$HOME/.local/bin/pip" ]; then
    PIP="$HOME/.local/bin/pip"
else
    PIP="pip"
fi

# --- Install huggingface_hub only (no torch!) ---
echo "[pip] Installing huggingface_hub..."
$PIP install --user --quiet huggingface_hub 2>/dev/null || \
$PIP install --user --break-system-packages --quiet huggingface_hub

# --- Download all models in PARALLEL ---
echo "[Downloads] Starting parallel downloads..."

# EasyOCR model 1
(
    echo "[EasyOCR] Downloading detection model (craft_mlt_25k.pth)..."
    curl -L -s -o "$MODEL_DIR/easyocr/model/craft_mlt_25k.pth" \
        "https://huggingface.co/xiaoyao9184/easyocr/resolve/master/craft_mlt_25k.pth"
    echo "[EasyOCR] craft_mlt_25k.pth done"
) &
PID_EASYOCR1=$!

# EasyOCR model 2
(
    echo "[EasyOCR] Downloading Latin recognition model (latin_g2.pth)..."
    curl -L -s -o "$MODEL_DIR/easyocr/model/latin_g2.pth" \
        "https://huggingface.co/xiaoyao9184/easyocr/resolve/master/latin_g2.pth"
    echo "[EasyOCR] latin_g2.pth done"
) &
PID_EASYOCR2=$!

# Whisper model
(
    echo "[Whisper] Downloading whisper-turbo model (~1.6GB)..."
    # URL from: whisper/__init__.py _MODELS["turbo"]
    curl -L -s -o "$MODEL_DIR/whisper/large-v3-turbo.pt" \
        "https://openaipublic.azureedge.net/main/whisper/models/aff26ae408abcba5fbf8813c21e62b0941638c5f6eebfb145be0c9839262a19a/large-v3-turbo.pt"
    echo "[Whisper] large-v3-turbo.pt done"
) &
PID_WHISPER=$!

# HuggingFace models (these use huggingface_hub which handles its own parallelism)
(
    echo "[HuggingFace] Downloading docling models..."
    HF_HOME="$MODEL_DIR/huggingface" python3 -c "
from huggingface_hub import snapshot_download
import concurrent.futures

repos = [
    'docling-project/docling-models',
    'docling-project/docling-layout-heron',
    'ibm-granite/granite-docling-258M',
]

def download(repo):
    print(f'[HuggingFace] Downloading {repo}...')
    snapshot_download(repo)
    print(f'[HuggingFace] {repo} done')

# Download HF models in parallel - one thread per repo
with concurrent.futures.ThreadPoolExecutor(max_workers=len(repos)) as executor:
    executor.map(download, repos)

print('[HuggingFace] All models done!')
"
) &
PID_HF=$!

# Wait for all downloads to complete
echo "[Downloads] Waiting for all downloads to complete..."
wait $PID_EASYOCR1 $PID_EASYOCR2 $PID_WHISPER $PID_HF
echo "[Downloads] All downloads complete!"

# --- Summary ---
echo ""
echo "=================================================="
echo "Models downloaded to: $MODEL_DIR"
du -sh "$MODEL_DIR/easyocr" "$MODEL_DIR/huggingface" "$MODEL_DIR/whisper" 2>/dev/null || true
echo ""
echo "To run container:"
echo "  docker run -d --gpus all -p 8000:8000 \\"
echo "    -v \$HOME/models:/models \\"
echo "    -e XDG_CACHE_HOME=/models \\"
echo "    bhosh/docling-serve-patched:v15"
echo "=================================================="
