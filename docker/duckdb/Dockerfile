FROM python:3.11

# Install apt dependencies
USER root
RUN apt-get update -qq \
    && apt-get install -y \
        gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG VERSION
RUN echo "Installing DuckDB version ${VERSION}"
RUN pip3 install --no-cache-dir fastapi uvicorn simplejson pytz duckdb==${VERSION}

# Setup the entrypoint
COPY entrypoint.sh /entrypoint.sh
COPY server.py /server.py
RUN set -x \
    && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh", "python", "/server.py"]

# Expose the DuckDB port
EXPOSE 5432
