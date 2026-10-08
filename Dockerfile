# ---------------------------------------------------------------------------
# Dockerfile – ONE image used by every Python/Spark part of the project
# (generator, MQTT bridge, Spark streaming job, Hive queries, MLlib, dashboard).
#
# Why Docker for everything? So you do NOT have to install Java, Spark or
# Hadoop "winutils" on Windows/Mac. The versions below are known to work
# together:
#     Apache Spark 3.5.1  (PySpark 3.5.1, Scala 2.12)
#     Java 17             (Eclipse Temurin, inside the image)
#     Python 3.10         (inside the image)
#     Kafka connector     spark-sql-kafka-0-10_2.12 : 3.5.1  (+ kafka-clients 3.4.1)
# ---------------------------------------------------------------------------

# Official Apache Spark image with Java 17 + Python 3.
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

# Install things as root (the base image normally runs as user "spark").
USER root

# --- Kafka connector for Spark Structured Streaming -------------------------
# These are exactly the jars that "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
# would download. We add them once at build time so every run works offline.
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

# --- Python libraries (pinned in requirements.txt) -------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# --- Runtime settings --------------------------------------------------------
# /app  = your project folder (mounted by docker-compose, so code edits apply instantly)
# /hive = Hive metastore (a Docker volume, see docker-compose.yml)
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DATA_DIR=/app/data \
    HIVE_DIR=/hive \
    PYSPARK_PYTHON=python3 \
    PATH="/opt/spark/bin:${PATH}"
WORKDIR /app

# Replace the Spark image's Kubernetes entrypoint with a plain shell command
# runner, so we stay root and can write into the mounted data folder.
ENTRYPOINT []
CMD ["bash"]
