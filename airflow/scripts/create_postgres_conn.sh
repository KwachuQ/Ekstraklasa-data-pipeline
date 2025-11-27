#!/usr/bin/env bash
set -e

echo "Creating PostgreSQL connection..."

if airflow connections get postgres_default >/dev/null 2>&1; then
  echo "Connection postgres_default already exists – skipping"
else
  airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema dwh \
    --conn-port 5432
  echo "PostgreSQL connection created successfully!"
fi