#!/usr/bin/env bash

echo "Building SingleStore version latest"

# Build docker image
if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/singlestore:latest --ulimit nofile=2048 .; then
    echo "Successfully built SingleStore version latest"
else
    echo "Failed to build SingleStore version latest" >&2
fi

# Login to docker registry
if docker login gitlab.db.in.tum.de:5005; then
    echo "Successfully logged in to docker registry"
else
    echo "Failed to login to docker registry" >&2
fi

# Push docker image
if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/singlestore:latest; then
    echo "Successfully pushed SingleStore version latest"
else
    echo "Failed to push SingleStore version latest" >&2
fi
