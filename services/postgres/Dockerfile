#FROM postgis/postgis
FROM postgres:latest

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    postgresql-server-dev-all \
    postgresql-12-rum \
    less \
    make \
    vim

RUN git clone https://github.com/postgrespro/rum.git /tmp/rum

WORKDIR /tmp/rum

RUN make USE_PGXS=1
RUN make USE_PGXS=1 install

WORKDIR /tmp/db

RUN mkdir /data && chown postgres /data

COPY schema.sql /docker-entrypoint-initdb.d/01.sql
