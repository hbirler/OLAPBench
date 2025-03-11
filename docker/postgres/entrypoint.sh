#!/usr/bin/env bash

# Create the local user
groupadd --gid ${HOST_GID} local
useradd --uid ${HOST_UID} --gid ${HOST_GID} local

# Switch to local user
exec gosu local "$@"
