#!/bin/bash
set -e

one_meg=1048576
mem_available=$(($( getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg))
cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)
disk_available=$(df / | tail -1 | awk '{print $4}')
warning_resources="false"

if (( mem_available < 4000 )); then
  echo "WARNING: Not enough memory. At least 4GB required. You have $(numfmt --to iec $((mem_available * one_meg)))"
  warning_resources="true"
fi
if (( cpus_available < 2 )); then
  echo "WARNING: Not enough CPUs. At least 2 recommended. You have ${cpus_available}"
  warning_resources="true"
fi
if (( disk_available < one_meg * 10 )); then
  echo "WARNING: Not enough disk. At least 10GB recommended. You have $(numfmt --to iec $((disk_available * 1024)))"
  warning_resources="true"
fi
if [[ ${warning_resources} == "true" ]]; then
  echo "WARNING: Insufficient resources to run Airflow!"
fi

mkdir -p /opt/airflow/logs /opt/airflow/dags
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/

/entrypoint airflow db migrate
/entrypoint airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true
