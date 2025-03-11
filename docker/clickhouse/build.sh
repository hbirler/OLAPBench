#!/usr/bin/env bash

# The versions of ClickHouse to build
versions=(
    24.8
    24.9
    24.10
    24.11
    24.12
    25.1
    25.2
    latest
)

for version in "${versions[@]}"; do
    echo "Building ClickHouse version ${version}"

    # Build docker image
    if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/clickhouse:${version} --ulimit nofile=2048 --build-arg VERSION=${version} .; then
        echo "Successfully built ClickHouse version ${version}"
    else
        echo "Failed to build ClickHouse version ${version}" >&2
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
    if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/clickhouse:${version}; then
        echo "Successfully pushed ClickHouse version ${version}"
    else
        echo "Failed to push ClickHouse version ${version}" >&2
    fi
done
