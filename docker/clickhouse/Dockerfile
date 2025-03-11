ARG VERSION=latest

FROM clickhouse:${VERSION}

USER root

# Install apt dependencies
RUN apt-get update -qq \
    && apt-get install -y \
        gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup entrypoint
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN set -x \
    && chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh", "/entrypoint.sh"]
