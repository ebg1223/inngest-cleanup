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

# Create postgresql.conf with required settings
RUN echo "shared_preload_libraries = 'pg_cron'" > /etc/postgresql/postgresql.conf \
    && echo "cron.database_name = 'public'" >> /etc/postgresql/postgresql.conf

# Create a script to copy settings during container initialization
RUN mkdir -p /docker-entrypoint-initdb.d \
    && echo '#!/bin/bash\n\
    cat /etc/postgresql/postgresql.conf >> $PGDATA/postgresql.conf\n\
    echo "CREATE EXTENSION pg_partman;" > /tmp/init.sql\n\
    echo "CREATE EXTENSION pg_cron;" >> /tmp/init.sql\n\
    psql -U postgres -d postgres -f /tmp/init.sql\n\
    psql -U postgres -d postgres -c "SELECT cron.schedule('"'"'partman-maintenance'"'"', '"'"'0 3 * * *'"'"', '"'"'SELECT partman.run_maintenance(p_analyze := true);'"'"');"' \
    > /docker-entrypoint-initdb.d/setup-extensions.sh \
    && chmod +x /docker-entrypoint-initdb.d/setup-extensions.sh