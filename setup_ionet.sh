#!/bin/bash
# Setup script for io.net Ray cluster deployment
# Run this after cloning the repo on io.net

set -e

echo "=== Setting up Docling Serve on io.net ==="

# 1. Build patched docling-parse
echo "Building patched docling-parse..."
cd docling-parse-patched
pip install pybind11 cmake
python local_build.py
pip install -e .
cd ..

# 2. Install docling-serve and dependencies
echo "Installing docling-serve..."
pip install docling-serve easyocr

# 3. Verify installation
echo "Verifying installation..."
python -c "import docling_parse; print('docling-parse:', docling_parse.__file__)"
python -c "import docling_serve; print('docling-serve OK')"

echo ""
echo "=== Setup complete! ==="
echo ""
echo "To start the server:"
echo "  DOCLING_SERVE_ENG_LOC_NUM_WORKERS=2 docling-serve run --host 0.0.0.0 --port 5001"
echo ""
echo "Your endpoint will be:"
echo "  https://exposed_service-[YOUR-SUFFIX].headnodes.io.systems/"
