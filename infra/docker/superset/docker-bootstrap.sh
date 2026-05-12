#!/usr/bin/env bash
# Simplified bootstrap for pre-built apache/superset image (no dev pip installs).
set -eo pipefail

if [[ "$DATABASE_DIALECT" == postgres* ]]; then
    echo "Installing psycopg2..."
    uv pip install --quiet psycopg2-binary
fi

echo "Installing Athena driver..."
uv pip install --quiet "pyathena[sqlalchemy]"

case "${1}" in
  worker)
    echo "Starting Celery worker..."
    celery --app=superset.tasks.celery_app:app worker -O fair -l INFO --concurrency=2
    ;;
  beat)
    echo "Starting Celery beat..."
    rm -f /tmp/celerybeat.pid
    celery --app=superset.tasks.celery_app:app beat \
      --pidfile /tmp/celerybeat.pid \
      -l INFO \
      -s "${SUPERSET_HOME:-/app/superset_home}/celerybeat-schedule"
    ;;
  app-gunicorn)
    echo "Starting web app..."
    /usr/bin/run-server.sh
    ;;
  *)
    echo "Unknown operation: ${1}"
    exit 1
    ;;
esac
