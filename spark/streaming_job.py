"""
spark/streaming_job.py
----------------------
PPT layer 3 - BIG DATA PLATFORM: Spark Structured Streaming + the data lake.

Reads the live "vitals" stream from Kafka and runs FOUR streaming queries:

  1. BRONZE  raw_vitals   - every Kafka message exactly as received (raw JSON string)
  2. SILVER  vitals       - parsed JSON, invalid readings dropped (noise filter),
                            duplicates removed, proper types
  3. GOLD    alerts       - one row per threshold alert (HR > 120, SpO2 < 92,
                            BP > 140/90, temp > 38, fall) with severity + latency
  4. GOLD    vitals_1min  - 1-minute window averages per patient (patient features
                            used by Hive queries and the MLlib risk model)

All layers are written as Parquet files, partitioned by date, under data/:
    data/bronze/raw_vitals/ingest_date=YYYY-MM-DD/part-....parquet
    data/silver/vitals/event_date=YYYY-MM-DD/...
    data/gold/alerts/event_date=YYYY-MM-DD/...
    data/gold/vitals_1min/event_date=YYYY-MM-DD/...

NOTE ON HDFS: on a laptop we write to local folders (data/bronze, data/silver,
data/gold) instead of an HDFS cluster. Spark writes through the same Hadoop
FileSystem API either way, so on a real cluster only the path would change
(e.g. hdfs://namenode:9000/lake/bronze/...).

Run (inside Docker):  docker compose up -d spark-streaming
Spark UI:             http://localhost:4040  (tab "Structured Streaming")
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings as S  # noqa: E402

# --------------------------------------------------------------------------
# Schema of one wearable reading (the JSON the simulator sends).
# Numbers are read as DOUBLE so both 72 and 72.0 are accepted.
# --------------------------------------------------------------------------
READING_SCHEMA = StructType([
    StructField("patient_id", StringType()),
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),       # "yyyy-MM-dd HH:mm:ss.SSS" in UTC
    StructField("heart_rate", DoubleType()),
    StructField("spo2", DoubleType()),
    StructField("systolic_bp", DoubleType()),
    StructField("diastolic_bp", DoubleType()),
    StructField("body_temp", DoubleType()),
    StructField("steps", LongType()),
    StructField("fall_detected", BooleanType()),
])


def build_spark():
    """Create a small local Spark session that fits on a laptop."""
    spark = (
        SparkSession.builder
        .appName("WearableHealth-Streaming")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))   # 2 CPU cores
        .config("spark.sql.shuffle.partitions", "4")          # default 200 is far too many for a laptop
        .config("spark.sql.session.timeZone", "UTC")          # device timestamps are UTC
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")                   # hide Spark's INFO noise
    return spark


def read_kafka(spark):
    """Source: subscribe to the Kafka topic 'vitals'."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", S.KAFKA_BOOTSTRAP)
        .option("subscribe", S.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")    # first run: also process readings sent before Spark started
        .option("maxOffsetsPerTrigger", 20000)    # cap each micro-batch so a backlog can't overload the laptop
        .option("failOnDataLoss", "false")        # don't crash if Kafka was reset during development
        .load()
    )


# --------------------------------------------------------------------------
# BRONZE: keep the raw message untouched (so we can always re-process later).
# --------------------------------------------------------------------------
def bronze_layer(kafka_df):
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_time"),
        F.to_date(F.col("timestamp")).alias("ingest_date"),
    )


# --------------------------------------------------------------------------
# Parse JSON + NOISE FILTER (drop missing values and impossible readings).
# --------------------------------------------------------------------------
def parse_and_clean(kafka_df):
    parsed = (
        kafka_df
        .select(F.col("value").cast("string").alias("raw_json"),
                F.col("timestamp").alias("kafka_time"))
        .withColumn("r", F.from_json("raw_json", READING_SCHEMA))   # bad JSON -> nulls
        .select("r.*", "kafka_time")
        .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss.SSS"))
        .drop("timestamp")
    )

    # A reading is VALID only if nothing is missing and every value is physically possible.
    valid = (
        F.col("patient_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("steps").isNotNull() & (F.col("steps") >= 0)
        & (F.col("systolic_bp") > F.col("diastolic_bp"))
    )
    for column, (low, high) in S.VALID_RANGES.items():
        valid = valid & F.col(column).between(low, high)          # null -> not valid

    return (
        parsed.filter(valid)
        .withColumn("fall_detected", F.coalesce(F.col("fall_detected"), F.lit(False)))
    )


# --------------------------------------------------------------------------
# SILVER: cleaned vitals, duplicates removed (same patient + same timestamp).
# --------------------------------------------------------------------------
def silver_layer(clean_df):
    return (
        clean_df
        .withWatermark("event_time", "2 minutes")                 # how long to remember readings for de-duplication
        .dropDuplicates(["patient_id", "event_time"])
        .withColumn("event_date", F.to_date("event_time"))
        .select("patient_id", "device_id", "event_time", "heart_rate", "spo2",
                "systolic_bp", "diastolic_bp", "body_temp", "steps", "fall_detected",
                "kafka_time", "event_date")
    )


# --------------------------------------------------------------------------
# GOLD - ALERTS: real-time threshold rules (anomaly detection).
# One reading can raise several alerts (e.g. fever AND high HR).
# --------------------------------------------------------------------------
def alerts_layer(clean_df):
    R = S.ALERT_RULES
    hr, spo2 = F.col("heart_rate"), F.col("spo2")
    sbp, dbp = F.col("systolic_bp"), F.col("diastolic_bp")
    temp, fall = F.col("body_temp"), F.col("fall_detected")

    def rule(alert_type, fires_when, critical_when, message):
        """Returns a struct when the rule fires, otherwise null."""
        return F.when(fires_when, F.struct(
            F.lit(alert_type).alias("alert_type"),
            F.when(critical_when, F.lit("CRITICAL")).otherwise(F.lit("WARNING")).alias("severity"),
            message.alias("message"),
        ))

    rules = [
        rule("HIGH_HR", hr > R["HIGH_HR"]["limit"], hr > R["HIGH_HR"]["critical"],
             F.format_string(f"Heart rate %.0f bpm (limit {R['HIGH_HR']['limit']})", hr)),
        rule("LOW_SPO2", spo2 < R["LOW_SPO2"]["limit"], spo2 < R["LOW_SPO2"]["critical"],
             F.format_string(f"SpO2 %.1f%% (limit {R['LOW_SPO2']['limit']}%%)", spo2)),
        rule("HIGH_BP",
             (sbp > R["HIGH_BP"]["sys_limit"]) | (dbp > R["HIGH_BP"]["dia_limit"]),
             (sbp > R["HIGH_BP"]["sys_critical"]) | (dbp > R["HIGH_BP"]["dia_critical"]),
             F.format_string(f"BP %.0f/%.0f mmHg (limit {R['HIGH_BP']['sys_limit']}/{R['HIGH_BP']['dia_limit']})", sbp, dbp)),
        rule("FEVER", temp > R["FEVER"]["limit"], temp > R["FEVER"]["critical"],
             F.format_string(f"Temperature %.1f C (limit {R['FEVER']['limit']})", temp)),
        rule("FALL", fall, F.lit(True), F.lit("Fall detected by wearable")),
    ]

    return (
        clean_df
        .withColumn("fired", F.filter(F.array(*rules), lambda a: a.isNotNull()))   # keep only rules that fired
        .withColumn("alert", F.explode("fired"))                                     # one row per alert
        .select(
            F.concat_ws("-", "patient_id", F.date_format("event_time", "yyyyMMddHHmmssSSS"),
                        F.col("alert.alert_type")).alias("alert_id"),
            "patient_id", "device_id", "event_time",
            F.col("alert.alert_type").alias("alert_type"),
            F.col("alert.severity").alias("severity"),
            F.col("alert.message").alias("message"),
            "heart_rate", "spo2", "systolic_bp", "diastolic_bp", "body_temp", "fall_detected",
            F.current_timestamp().alias("alert_time"),
        )
        # How long from the device reading to the alert being raised (seconds).
        .withColumn("latency_sec",
                    F.round(F.col("alert_time").cast("double") - F.col("event_time").cast("double"), 2))
        .withColumn("event_date", F.to_date("event_time"))
    )


# --------------------------------------------------------------------------
# GOLD - 1-MINUTE WINDOWS per patient (patient features).
# The watermark says "readings more than 1 minute late are ignored", which lets
# Spark close each window and write it once (append mode).
# --------------------------------------------------------------------------
def windows_layer(clean_df):
    R = S.ALERT_RULES
    abnormal = (
        (F.col("heart_rate") > R["HIGH_HR"]["limit"])
        | (F.col("spo2") < R["LOW_SPO2"]["limit"])
        | (F.col("systolic_bp") > R["HIGH_BP"]["sys_limit"])
        | (F.col("diastolic_bp") > R["HIGH_BP"]["dia_limit"])
        | (F.col("body_temp") > R["FEVER"]["limit"])
        | F.col("fall_detected")
    )
    return (
        clean_df
        .withWatermark("event_time", "1 minute")
        .groupBy(F.window("event_time", "1 minute"), F.col("patient_id"))
        .agg(
            F.count(F.lit(1)).alias("readings"),
            F.round(F.avg("heart_rate"), 1).alias("avg_hr"),
            F.max("heart_rate").alias("max_hr"),
            F.round(F.avg("spo2"), 2).alias("avg_spo2"),
            F.min("spo2").alias("min_spo2"),
            F.round(F.avg("systolic_bp"), 1).alias("avg_sys"),
            F.round(F.avg("diastolic_bp"), 1).alias("avg_dia"),
            F.max("systolic_bp").alias("max_sys"),
            F.round(F.avg("body_temp"), 2).alias("avg_temp"),
            F.max("body_temp").alias("max_temp"),
            (F.max("steps") - F.min("steps")).alias("steps_in_window"),
            F.sum(F.col("fall_detected").cast("int")).alias("falls"),
            F.sum(abnormal.cast("int")).alias("abnormal_readings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "patient_id", "readings", "avg_hr", "max_hr", "avg_spo2", "min_spo2",
            "avg_sys", "avg_dia", "max_sys", "avg_temp", "max_temp",
            "steps_in_window", "falls", "abnormal_readings",
            F.to_date(F.col("window.start")).alias("event_date"),
        )
    )


def write_parquet(df, name, path, trigger, partition_col):
    """Start one streaming query that appends Parquet files to `path`."""
    return (
        df.writeStream
        .queryName(name)
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", os.path.join(S.CHECKPOINT_DIR, name))   # exactly-once bookkeeping
        .partitionBy(partition_col)
        .outputMode("append")
        .trigger(processingTime=trigger)
        .start()
    )


def main():
    spark = build_spark()
    print("=" * 70)
    print(f"Spark {spark.version} streaming job started")
    print(f"  Kafka      : {S.KAFKA_BOOTSTRAP}  topic '{S.KAFKA_TOPIC}'")
    print(f"  Bronze     : {S.BRONZE_RAW}")
    print(f"  Silver     : {S.SILVER_VITALS}")
    print(f"  Gold       : {S.GOLD_ALERTS}")
    print(f"               {S.GOLD_VITALS_1MIN}")
    print(f"  Checkpoints: {S.CHECKPOINT_DIR}")
    print("=" * 70)

    kafka_df = read_kafka(spark)
    clean_df = parse_and_clean(kafka_df)

    # coalesce/repartition(1) = one Parquet file per micro-batch (avoids thousands of tiny files)
    queries = [
        write_parquet(bronze_layer(kafka_df).coalesce(1), "bronze_raw_vitals",
                      S.BRONZE_RAW, "10 seconds", "ingest_date"),
        write_parquet(silver_layer(clean_df).repartition(1), "silver_vitals",
                      S.SILVER_VITALS, "5 seconds", "event_date"),
        write_parquet(alerts_layer(clean_df).coalesce(1), "gold_alerts",
                      S.GOLD_ALERTS, "3 seconds", "event_date"),          # short trigger = fast alerts
        write_parquet(windows_layer(clean_df).repartition(1), "gold_vitals_1min",
                      S.GOLD_VITALS_1MIN, "10 seconds", "event_date"),
    ]

    # Every 30 s print a one-line status per query, until one of them stops/fails.
    while not spark.streams.awaitAnyTermination(30):
        for q in queries:
            p = q.lastProgress
            rows = p["numInputRows"] if p else 0
            print(f"[streaming] {q.name:<18} batch={p['batchId'] if p else '-':<6} "
                  f"rows_in_last_batch={rows:<6} status={q.status['message']}")


if __name__ == "__main__":
    main()
