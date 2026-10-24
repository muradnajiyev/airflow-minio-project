FROM apache/airflow:3.1.0

USER root

RUN apt-get update && \
    apt-get install -y git bash curl unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=heifeng/mc:RELEASE.2025-04-16T18-13-26Z /usr/bin/mc /usr/local/bin/mc

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt