"""
spark/check_layers.py
---------------------
A quick "is it working?" check for Phase 4. Reads each layer of the data lake
(Bronze / Silver / Gold) with a normal Spark batch job and prints row counts
plus a few sample rows. Good to show in the viva.

Run (while the streaming job is running):
    docker compose exec spark-streaming spark-submit spark/check_layers.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings as S  # noqa: E402


def read(spark, path):
    """Read a Parquet folder written by the streaming job (None if it doesn't exist yet)."""
    if not os.path.exists(path):
        return None
    try:
        return spark.read.parquet(path)
    except Exception as exc:   # e.g. folder exists but no file committed yet
        print(f"  (could not read {path}: {exc.__class__.__name__})")
        return None


def main():
    spark = (SparkSession.builder.appName("WearableHealth-CheckLayers")
             .master("local[1]").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    bronze = read(spark, S.BRONZE_RAW)
    silver = read(spark, S.SILVER_VITALS)
    alerts = read(spark, S.GOLD_ALERTS)
    windows = read(spark, S.GOLD_VITALS_1MIN)

    print("\n==================== DATA LAKE CHECK ====================")
    n_bronze = bronze.count() if bronze is not None else 0
    n_silver = silver.count() if silver is not None else 0
    print(f"BRONZE raw messages        : {n_bronze}")
    print(f"SILVER clean readings      : {n_silver}")
    if n_bronze:
        print(f"   -> dropped by noise filter (invalid / not yet processed): {n_bronze - n_silver}")

    if silver is not None:
        print("\nSILVER sample (latest 5 readings):")
        silver.orderBy(F.col("event_time").desc()).show(5, truncate=False)

    if alerts is not None:
        print("GOLD alerts by type:")
        alerts.groupBy("alert_type", "severity").count().orderBy("alert_type", "severity").show(truncate=False)
        print("GOLD latest 5 alerts:")
        (alerts.orderBy(F.col("event_time").desc())
         .select("event_time", "patient_id", "alert_type", "severity", "message", "latency_sec")
         .show(5, truncate=False))
    else:
        print("\nGOLD alerts: none yet")

    if windows is not None:
        print(f"GOLD 1-minute windows      : {windows.count()}")
        (windows.orderBy(F.col("window_start").desc())
         .select("window_start", "patient_id", "readings", "avg_hr", "avg_spo2",
                 "avg_sys", "avg_dia", "avg_temp", "abnormal_readings")
         .show(5, truncate=False))
    else:
        print("GOLD 1-minute windows: none yet (the first windows appear ~2 minutes after start)")

    spark.stop()


if __name__ == "__main__":
    main()
