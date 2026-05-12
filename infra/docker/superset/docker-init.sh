#!/usr/bin/env bash
set -e

ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"

echo "Installing psycopg2..."
uv pip install --quiet psycopg2-binary

echo "Installing Athena driver..."
uv pip install --quiet "pyathena[sqlalchemy]"

echo "Step 1: Applying DB migrations"
superset db upgrade

echo "Step 2: Creating admin user (admin / ${ADMIN_PASSWORD})"
superset fab create-admin \
    --username admin \
    --email admin@superset.com \
    --password "${ADMIN_PASSWORD}" \
    --firstname Superset \
    --lastname Admin

echo "Step 3: Setting up roles and permissions"
superset init

if [ "${SUPERSET_LOAD_EXAMPLES}" = "yes" ]; then
    echo "Step 4: Loading example dashboards"
    superset load_examples
fi
