#!/usr/bin/env bash

# Initialize the database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com

# Start the web server
exec "$@"
