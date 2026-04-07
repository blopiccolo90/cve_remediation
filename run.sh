#!/bin/bash
# Script di lancio per la pipeline CVE Remediation

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "🔧 CVE Remediation Pipeline"
echo "=============================="

# Controlla se PySpark è installato
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "⚠️  PySpark non è installato."
    echo ""
    echo "Installa le dipendenze:"
    echo "  pip install --break-system-packages -r requirements.txt"
    echo ""
    exit 1
fi

echo "✅ Dipendenze trovate"
echo ""
echo "▶️  Esecuzione main.py..."
echo "=============================="
echo ""

python3 -m src.main

echo ""
echo "=============================="
echo "✅ Esecuzione completata"
