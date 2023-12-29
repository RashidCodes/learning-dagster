#!/bin/bash 

# Start a postgres container
docker run \
    --name dagster-postgres \
    -e POSTGRES_USER=kingmoh \
    -e POSTGRES_PASSWORD=mysecretpassword \
    -e POSTGRES_DB=dagster-runs \
    -d postgres