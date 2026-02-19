#!/bin/bash
# ============================================================
#  Healthcare Data Platform — Mac Setup Script
#  Run this once from the project root:
#    chmod +x setup.sh && ./setup.sh
# ============================================================

set -e  # Exit on any error

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo ""
echo "============================================================"
echo "  HEALTHCARE PLATFORM SETUP"
echo "  Project dir: $PROJECT_DIR"
echo "============================================================"

# ── 1. Check Python ──────────────────────────────────────────
echo ""
echo "→ Checking Python..."
if ! command -v python3 &>/dev/null; then
    echo "  ERROR: python3 not found. Install it from https://python.org"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
echo "  ✓ $PYTHON_VERSION"

# ── 2. Create virtual environment ───────────────────────────
echo ""
echo "→ Creating virtual environment at $PROJECT_DIR/venv ..."
if [ -d "$PROJECT_DIR/venv" ]; then
    echo "  (venv already exists — skipping)"
else
    python3 -m venv "$PROJECT_DIR/venv"
    echo "  ✓ Virtual environment created"
fi

# ── 3. Install dependencies ──────────────────────────────────
echo ""
echo "→ Installing dependencies..."
"$PROJECT_DIR/venv/bin/pip" install --quiet --upgrade pip
"$PROJECT_DIR/venv/bin/pip" install --quiet -r "$PROJECT_DIR/requirements.txt"
echo "  ✓ pandas, numpy + flask installed"

# ── 4. Run the pipeline ──────────────────────────────────────
echo ""
echo "→ Running the local pipeline..."
echo ""
cd "$PROJECT_DIR"
"$PROJECT_DIR/venv/bin/python" scripts/run_pipeline_local.py

echo ""
echo "============================================================"
echo "  SETUP COMPLETE!"
echo ""
echo "  Run the pipeline:"
echo "    cd $PROJECT_DIR"
echo "    ./venv/bin/python3 scripts/run_pipeline_local.py"
echo ""
echo "  Launch the dashboard:"
echo "    ./venv/bin/python3 scripts/dashboard_server.py"
echo "    → then open http://localhost:5050"
echo "============================================================"
echo ""
