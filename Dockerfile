# Custom Airflow image: base + Java 17 (for PySpark) + pyspark.
# Used by docker-compose.yaml via `build: .`. Rebuild with:
#   docker compose -f docker-compose.yaml --env-file .env.airflow build
FROM apache/airflow:3.3.1

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
# Spark needs JAVA_HOME; path is the Debian bookworm amd64 openjdk-17 location.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
# pyspark 4.x is the first line that supports Python 3.13 (the base image's Python).
# The repo pins pyspark==3.5.1 for the local venv; the container needs a 3.13-compatible build.
RUN pip install --no-cache-dir "pyspark==4.1.3"
