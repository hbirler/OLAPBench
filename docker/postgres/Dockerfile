ARG VERSION=latest

FROM postgres:${VERSION}

USER root

# Install apt dependencies
RUN apt-get update -qq || true \
    && apt-get install -y \
        gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN set -x \
    && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh", "docker-entrypoint.sh"]

# Expose the Postgres port
EXPOSE 5432
