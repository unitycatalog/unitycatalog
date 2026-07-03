#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "oauth-server";
    CREATE DATABASE team;
    CREATE DATABASE authentication;
EOSQL
