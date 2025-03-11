#!/usr/bin/env bash

# The versions of Hyper to build
versions=(
    0.0.21200
    latest
)

for version in "${versions[@]}"; do
    if [ "$version" = "latest" ]; then
        hyper_version=$(pip index versions tableauhyperapi 2>/dev/null | grep -oP 'Available versions: \K[^,]+' | head -1)
    else
        hyper_version=$version
    fi

    echo "Building Hyper version ${version} (${hyper_version})"
    # Build docker image
    if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/hyper:${version} --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 --build-arg VERSION="${hyper_version}" .; then
        echo "Successfully built Hyper version ${version}"
    else
        echo "Failed to build Hyper version ${version}" >&2
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
    if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/hyper:${version}; then
        echo "Successfully pushed Hyper version ${version}"
    else
        echo "Failed to push Hyper version ${version}" >&2
    fi
done
