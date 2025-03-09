FROM postgres:17

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-all \
    git \
    clang \
    && git clone https://github.com/pgpartman/pg_partman.git /tmp/pg_partman \
    && cd /tmp/pg_partman \
    && make && make install \
    # Install pg_cron from source
    && git clone https://github.com/citusdata/pg_cron.git /tmp/pg_cron \
    && cd /tmp/pg_cron \
    && make && make install \
    && rm -rf /tmp/pg_partman /tmp/pg_cron \
    && apt-get remove -y build-essential git clang \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copy a custom postgresql.conf with the required pg_cron settings
COPY postgresql.conf /etc/postgresql/postgresql.conf

# Create a docker-entrypoint-initdb.d script to set up pg_cron
RUN echo "#!/bin/bash\necho \"shared_preload_libraries = 'pg_cron'\" >> /var/lib/postgresql/data/postgresql.conf\necho \"cron.database_name = 'postgres'\" >> /var/lib/postgresql/data/postgresql.conf" > /docker-entrypoint-initdb.d/setup-pg-cron.sh \
    && chmod +x /docker-entrypoint-initdb.d/setup-pg-cron.sh