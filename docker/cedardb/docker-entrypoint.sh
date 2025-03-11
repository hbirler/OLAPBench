#!/usr/bin/env bash

set -xeuo pipefail

CEDARDB_DATA=/var/lib/cedardb/data

# Setup a database
test -d "$CEDARDB_DATA/database" || /usr/local/bin/cedardb/bin/sql -createdb "$CEDARDB_DATA/database" <<<"ALTER ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres';" || exit 1

# Start the server
exec /usr/local/bin/cedardb/bin/server "$CEDARDB_DATA/database" -address=0.0.0.0 "-port=5432" || exit 1
