#!/usr/bin/env bash
set -euo pipefail

# Run the local Namecheap CSV -> Ahrefs script on VPS.
# Usage:
#   ./scripts/run_namecheap_vps.sh /path/to/namecheap.csv [output.csv] [config.yaml]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /path/to/namecheap.csv [output.csv] [config.yaml]"
  exit 1
fi

INPUT_CSV="$1"
OUTPUT_CSV="${2:-}"
CONFIG_PATH="${3:-}"

if [[ ! -f "$INPUT_CSV" ]]; then
  echo "Input CSV not found: $INPUT_CSV"
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required but not installed."
  exit 1
fi

# Optional convenience: export vars from .env (repo root and/or scripts/ next to this file).
_load_env_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  set -a
  # shellcheck disable=SC1091
  source "$f"
  set +a
}
_load_env_file "$REPO_ROOT/.env"
_load_env_file "$SCRIPT_DIR/.env"

if [[ -z "${AHREFS_API_KEY:-}" ]]; then
  echo "AHREFS_API_KEY is not set. Put it in .env or export it in shell."
  exit 1
fi

ARGS=(scripts/namecheap_csv_ahrefs.py "$INPUT_CSV")
if [[ -n "$OUTPUT_CSV" ]]; then
  ARGS+=(-o "$OUTPUT_CSV")
fi
if [[ -n "$CONFIG_PATH" ]]; then
  ARGS+=(-c "$CONFIG_PATH")
fi

python3 "${ARGS[@]}"
