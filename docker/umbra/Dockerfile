ARG VERSION=latest

FROM umbradb/umbra:${VERSION}

# Install apt dependencies
USER root
RUN apt-get update -qq \
    && apt-get install -y \
        gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup the entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN set -x \
    && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh", "/docker-entrypoint.sh"]

# Expose the Umbra port
EXPOSE 5432
