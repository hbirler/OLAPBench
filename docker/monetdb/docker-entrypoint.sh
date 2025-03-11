#!/usr/bin/env bash

set -e -o pipefail -o nounset
set +x

# Create the dbfarm
mkdir -p /var/monetdb5/dbfarm
monetdbd create /var/monetdb5/dbfarm

# Create the database
monetdbd start /var/monetdb5/dbfarm
monetdb create main
monetdb release main
monetdbd stop /var/monetdb5/dbfarm

# Enable control
monetdbd set passphrase="monetdb" /var/monetdb5/dbfarm
monetdbd set control=true /var/monetdb5/dbfarm
monetdbd set listenaddr=all /var/monetdb5/dbfarm

# Start the server
exec monetdbd start -n /var/monetdb5/dbfarm