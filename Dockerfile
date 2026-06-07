# vim: set ft=dockerfile tw=88 nu ai et ts=4 sw=4:

# PySpark requires both Python and a JVM (>= 17).
# Using the official uv image — consistent with local development tooling.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# System dependencies: JDK for Spark, procps for Spark's internal ps calls.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Spark binds to the loopback interface inside the container.
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_DIRS=/tmp/spark-local

WORKDIR /app

# Install dependencies via uv — faster and reproducible via uv.lock.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen \
    && mkdir -p /tmp/spark-local \
    && chmod 777 /tmp/spark-local

# Application source
COPY src/ ./src/

# Mount points — data and outputs live outside the image.
VOLUME ["/app/data_arch", "/app/outputs"]

ENV AIS_DATA_GLOB=data_arch/aisdk-2021-12/*.csv
ENV AIS_OUTPUT_DIR=outputs
ENV SPARK_DRIVER_MEMORY=8g

CMD ["uv", "run", "python", "-m", "src.main"]
