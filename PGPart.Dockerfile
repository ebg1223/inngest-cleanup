FROM postgres:17

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-all \
    git \
    clang \
    && git clone https://github.com/pgpartman/pg_partman.git /tmp/pg_partman \
    && cd /tmp/pg_partman \
    && make && make install \
    && rm -rf /tmp/pg_partman \
    && apt-get remove -y build-essential git clang \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
