FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Install apt dependencies
RUN apt-get update -qq \
    && apt-get install -y \
        gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN set -x \
    && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh", "/opt/mssql/bin/sqlservr"]
