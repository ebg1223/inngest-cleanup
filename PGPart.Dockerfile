FROM postgres:17

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-all \
    git \
    clang \
    && git clone https://github.com/pgpartman/pg_partman.git /tmp/pg_partman \
    && cd /tmp/pg_partman \
    && make && make install \
    && git clone https://github.com/citusdata/pg_cron.git /tmp/pg_cron \
    && cd /tmp/pg_cron \
    && make && make install \
    && rm -rf /tmp/pg_partman /tmp/pg_cron \
    && apt-get remove -y build-essential git clang \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
