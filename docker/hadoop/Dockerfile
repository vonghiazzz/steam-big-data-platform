# BDA501 Hadoop Lab image
# Hadoop 3.5.0 official ASF image + Python 3 for Hadoop Streaming exercises.
ARG HADOOP_IMAGE=ghcr.io/apache/hadoop:3.5.0
FROM ${HADOOP_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data/name /data/data /workspace \
    && chown -R hadoop:hadoop /data /workspace

USER hadoop
WORKDIR /workspace
