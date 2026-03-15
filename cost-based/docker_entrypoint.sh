#!/bin/sh
# Umbrel-compatible entrypoint for cost-based
# Reads configuration entirely from environment variables (no StartOS config.yaml).

set -e

# Health check mode (called by docker HEALTHCHECK)
if [ "$1" = "check-web" ]; then
    curl -sf http://localhost:5000/api/health > /dev/null 2>&1 || exit 1
    exit 0
fi

# Set defaults for all env vars the app reads.
# These can be overridden in docker-compose.yml.
export DATABASE_PATH="${DATABASE_PATH:-/data/cost_basis.db}"
export ELECTRS_HOST="${ELECTRS_HOST:-10.21.21.10}"
export ELECTRS_PORT="${ELECTRS_PORT:-50001}"
export MEMPOOL_URL="${MEMPOOL_URL:-http://10.21.21.10:3006}"
export DEFAULT_CURRENCY="${DEFAULT_CURRENCY:-USD}"
export GAP_LIMIT="${GAP_LIMIT:-20}"
export COST_BASIS_METHOD="${COST_BASIS_METHOD:-HIFO}"
export TOR_PROXY_HOST="${TOR_PROXY_HOST:-}"
export TOR_PROXY_PORT="${TOR_PROXY_PORT:-}"

# Ensure the data directory exists
mkdir -p /data

printf "\n [i] Starting Cost Based (Umbrel)...\n"
printf " [i] Electrs: %s:%s\n" "$ELECTRS_HOST" "$ELECTRS_PORT"
printf " [i] Mempool: %s\n" "$MEMPOOL_URL"
printf " [i] Currency: %s | Gap Limit: %s | Method: %s\n\n" \
    "$DEFAULT_CURRENCY" "$GAP_LIMIT" "$COST_BASIS_METHOD"

exec tini -- gunicorn \
    --bind 0.0.0.0:5000 \
    --workers 2 \
    --timeout 120 \
    app:app
