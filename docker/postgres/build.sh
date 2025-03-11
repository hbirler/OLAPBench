#!/usr/bin/env bash

# The versions of Postgres to build
versions=(
    12.0
    13.0
    14.0
    15.0
    16.0
    17.0
    latest
)

for version in "${versions[@]}"; do
    echo "Building Postgres version ${version}"

    # Build docker image
    if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/postgres:${version} --ulimit nofile=2048 --build-arg VERSION=${version} .; then
        echo "Successfully built Postgres version ${version}"
    else
        echo "Failed to build Postgres version ${version}" >&2
        continue
    fi

    # Login to docker registry
    if docker login gitlab.db.in.tum.de:5005; then
        echo "Successfully logged in to docker registry"
    else
        echo "Failed to login to docker registry" >&2
        continue
    fi

    # Push docker image
    if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/postgres:${version}; then
        echo "Successfully pushed Postgres version ${version}"
    else
        echo "Failed to push Postgres version ${version}" >&2
    fi
done
