#!/usr/bin/env bash
set -euo pipefail

# Run the local Namecheap CSV -> Ahrefs script on VPS.
# Usage:
#   ./scripts/check_namecheap.sh /path/to/namecheap.csv [output.csv] [config.yaml]

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

# Optional convenience: export KEY=value lines from .env (dotenv-style).
# Uses a small parser so spaces around "=" work; plain `source .env` treats `KEY = val` as a command.
_load_env_file() {
  local f="$1" line key val
  [[ -f "$f" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    if [[ "$line" =~ ^(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=[[:space:]]*(.*)$ ]]; then
      key="${BASH_REMATCH[2]}"
      val="${BASH_REMATCH[3]}"
    elif [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*:[[:space:]]+(.+)$ ]]; then
      key="${BASH_REMATCH[1]}"
      val="${BASH_REMATCH[2]}"
    else
      continue
    fi
    if [[ "$val" =~ ^\"(.*)\"$ ]]; then
      val="${BASH_REMATCH[1]}"
    elif [[ "$val" =~ ^\'(.*)\'$ ]]; then
      val="${BASH_REMATCH[1]}"
    fi
    export "${key}=${val}"
  done < "$f"
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
