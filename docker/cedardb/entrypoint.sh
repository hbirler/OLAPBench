#!/usr/bin/env bash

# Create the local user
groupadd --gid ${HOST_GID} local
useradd --uid ${HOST_UID} --gid ${HOST_GID} local

chown local:local /var/lib/cedardb/data

# Switch to local user
exec gosu local "$@"
