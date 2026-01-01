FROM apache/superset:latest

USER root

RUN apt-get update && \
    apt-get install -y \
        pkg-config \
        default-libmysqlclient-dev \
        build-essential && \
    /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir pymysql mysqlclient

USER superset

