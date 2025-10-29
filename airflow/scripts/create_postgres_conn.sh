#!/bin/bash
echo "Creating PostgreSQL connection..."
airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema dwh \
    --conn-port 5432
echo "PostgreSQL connection created successfully!"