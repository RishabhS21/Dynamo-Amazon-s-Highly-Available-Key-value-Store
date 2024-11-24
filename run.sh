#!/bin/bash

# Exit script on any error
set -e

# Database user
DB_USER="postgres"

# Get the list of databases excluding postgres, template0, and template1
databases=$(sudo -u "$DB_USER" psql -t -c "SELECT datname FROM pg_database WHERE datname NOT IN ('postgres', 'template0', 'template1');")

# Iterate over each database and drop it
for db in $databases; do
  # Remove leading/trailing whitespace
  db=$(echo $db | xargs)
  echo "Dropping database: $db"
  sudo -u "$DB_USER" psql -c "DROP DATABASE IF EXISTS $db;"
done

echo "All non-system databases have been deleted."
$(rm logs/server.log)
$(python3 -m main)