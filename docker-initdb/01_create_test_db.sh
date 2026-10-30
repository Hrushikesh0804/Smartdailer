#!/usr/bin/env bash
# Creates the test database when the Postgres container first starts.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE smartdialer_test'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'smartdialer_test')\gexec
EOSQL
