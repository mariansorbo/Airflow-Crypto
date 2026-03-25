#!/bin/bash
# ============================================================
# migrate.sh — applies pending SQL migrations to crypto_data
#
# Usage:
#   bash sql/migrate.sh                    # applies all pending
#   bash sql/migrate.sh --dry-run          # shows what would run
#
# Migrations live in sql/migrations/ and follow the naming
# convention:  V{number}__{description}.sql
# Each file is applied exactly once; applied versions are
# tracked in the schema_migrations table inside crypto_data.
# ============================================================

set -euo pipefail

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-crypto_data}"
DB_USER="${DB_USER:-airflow}"
DB_PASS="${DB_PASS:-airflow}"
MIGRATIONS_DIR="$(dirname "$0")/migrations"
DRY_RUN=false

for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

export PGPASSWORD="$DB_PASS"
PSQL="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"

# ── Ensure tracking table exists ─────────────────────────────────────────────
$PSQL -q <<'EOF'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(20)  PRIMARY KEY,
    filename    TEXT         NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
EOF

echo "── Crypto Market Snapshots — DB Migrations ──────────────────────────────"

# ── Apply pending migrations in order ────────────────────────────────────────
applied=0
for filepath in $(ls "$MIGRATIONS_DIR"/V*.sql | sort -V); do
    filename=$(basename "$filepath")
    version=$(echo "$filename" | grep -oP '^V\d+')

    already_applied=$($PSQL -tAq \
        -c "SELECT COUNT(*) FROM schema_migrations WHERE version = '$version'")

    if [[ "$already_applied" -gt 0 ]]; then
        echo "  [skip]  $filename  (already applied)"
        continue
    fi

    if [[ "$DRY_RUN" == true ]]; then
        echo "  [dry]   $filename  (would apply)"
        continue
    fi

    echo "  [apply] $filename ..."
    $PSQL -q -f "$filepath"
    $PSQL -q \
        -c "INSERT INTO schema_migrations (version, filename) VALUES ('$version', '$filename')"
    echo "          done"
    ((applied++)) || true
done

echo "────────────────────────────────────────────────────────────────────────"
if [[ "$DRY_RUN" == true ]]; then
    echo "  Dry run complete — no changes applied"
else
    echo "  $applied migration(s) applied"
fi
