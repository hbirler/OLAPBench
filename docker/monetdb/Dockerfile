FROM ubuntu:24.10

# Install apt dependencies
RUN apt-get update -qq \
    && apt-get install -y  \
        lsb-release \
        gnupg2 \
        gosu \
        wget \
    && wget --quiet -O - https://dev.monetdb.org/downloads/MonetDB-GPG-KEY.gpg | apt-key add - \
    && sh -c 'echo "deb https://dev.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb" > /etc/apt/sources.list.d/monetdb.list' \
    && sh -c 'echo "deb-src https://dev.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb" >> /etc/apt/sources.list.d/monetdb.list' \
    && apt-get update -qq \
    && apt-get install -y \
        monetdb5-sql \
        monetdb-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup entrypoint
COPY entrypoint.sh /entrypoint.sh
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN set -x \
    && chmod +x /entrypoint.sh \
    && chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/entrypoint.sh", "/docker-entrypoint.sh"]