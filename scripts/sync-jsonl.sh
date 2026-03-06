#!/usr/bin/env bash
# sync-jsonl.sh — Copy the JSONL knowledge base into webapp/src/data/
#
# Usage:
#   ./scripts/sync-jsonl.sh /path/to/gendwh_knowledge.jsonl

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <path-to-jsonl-file>"
  exit 1
fi

SRC="$1"
DEST_DIR="$(dirname "$0")/../webapp/src/data"

if [ ! -f "$SRC" ]; then
  echo "ERROR: File not found: $SRC"
  exit 1
fi

mkdir -p "$DEST_DIR"
cp "$SRC" "$DEST_DIR/gendwh_knowledge.jsonl"
echo "✔ Copied $(basename "$SRC") → $DEST_DIR/gendwh_knowledge.jsonl"

