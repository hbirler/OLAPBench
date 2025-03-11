#!/usr/bin/env bash

echo "Building SQLServer version latest"

# Build docker image
if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/sqlserver:latest --ulimit nofile=2048 .; then
    echo "Successfully built SQLServer version latest"
else
    echo "Failed to build SQLServer version latest" >&2
fi

# Login to docker registry
if docker login gitlab.db.in.tum.de:5005; then
    echo "Successfully logged in to docker registry"
else
    echo "Failed to login to docker registry" >&2
fi

# Push docker image
if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/sqlserver:latest; then
    echo "Successfully pushed SQLServer version latest"
else
    echo "Failed to push SQLServer version latest" >&2
fi
