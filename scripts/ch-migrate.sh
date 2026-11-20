#!/usr/bin/env bash
# Apply every migrations/clickhouse/*.sql file in lexical order using
# clickhouse-client --multiquery. Intended to run in the one-shot
# clickhouse-migrate Compose service, but works on any machine with
# clickhouse-client installed.
#
# All statements use CREATE ... IF NOT EXISTS, so re-running is safe.
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-/migrations}"

if [ ! -d "${MIGRATIONS_DIR}" ]; then
  echo "migrations directory not found: ${MIGRATIONS_DIR}" >&2
  exit 1
fi

client=(clickhouse-client --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT}" --user "${CLICKHOUSE_USER}")
if [ -n "${CLICKHOUSE_PASSWORD}" ]; then
  client+=(--password "${CLICKHOUSE_PASSWORD}")
fi

shopt -s nullglob
files=("${MIGRATIONS_DIR}"/*.sql)
if [ "${#files[@]}" -eq 0 ]; then
  echo "no .sql files in ${MIGRATIONS_DIR}" >&2
  exit 1
fi

for file in "${files[@]}"; do
  echo "applying ${file}"
  "${client[@]}" --multiquery < "${file}"
done

echo "clickhouse migrations applied"
