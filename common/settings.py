"""
common/settings.py
------------------
ONE place for every setting the project shares:
  * where the data lake lives (Bronze / Silver / Gold folders)
  * the Kafka topic name and broker address
  * the clinical alert thresholds (used by Spark, the dashboard and the tests)
  * the risk-scoring points (used by the MLlib model to create labels)

Every other file imports from here, so if you change a threshold it changes
everywhere. Only the Python standard library is used, so this file can be
imported by the generator, by Spark jobs and by the dashboard alike.
"""

import os

# --------------------------------------------------------------------------
# 1. Paths of the data lake
# --------------------------------------------------------------------------
# Inside Docker the project folder is mounted at /app, so data goes to
# /app/data, which is the "data" folder in your project on your laptop.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(PROJECT_ROOT, "data"))

# Medallion layers (the PPT's HDFS Bronze / Silver / Gold), stored as Parquet.
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")   # raw JSON exactly as received
SILVER_DIR = os.path.join(DATA_DIR, "silver")   # cleaned, typed vitals
GOLD_DIR = os.path.join(DATA_DIR, "gold")       # patient features, alerts, risk

BRONZE_RAW = os.path.join(BRONZE_DIR, "raw_vitals")
SILVER_VITALS = os.path.join(SILVER_DIR, "vitals")
GOLD_VITALS_1MIN = os.path.join(GOLD_DIR, "vitals_1min")      # 1-minute window averages per patient
GOLD_ALERTS = os.path.join(GOLD_DIR, "alerts")                # threshold alerts
GOLD_PATIENT_RISK = os.path.join(GOLD_DIR, "patient_risk")    # MLlib risk level per patient
GOLD_MODEL_METRICS = os.path.join(GOLD_DIR, "model_metrics.json")

# Reference data (the "Patients" table in the PPT's Hive box).
REFERENCE_DIR = os.path.join(DATA_DIR, "reference")
PATIENTS_DIR = os.path.join(REFERENCE_DIR, "patients")
PATIENTS_CSV = os.path.join(PATIENTS_DIR, "patients.csv")

# Spark streaming checkpoints (Spark's "bookmark" of what it already processed).
CHECKPOINT_DIR = os.path.join(DATA_DIR, "checkpoints")

# Saved MLlib model.
MODEL_DIR = os.path.join(DATA_DIR, "models", "risk_decision_tree")

# Hive metastore + warehouse (kept in a Docker volume, see docker-compose.yml).
HIVE_DIR = os.environ.get("HIVE_DIR", os.path.join(DATA_DIR, "hive"))
HIVE_DATABASE = "wearable_health"

# --------------------------------------------------------------------------
# 2. Kafka / MQTT
# --------------------------------------------------------------------------
# Inside Docker containers Kafka is reached as "kafka:29092".
# From your laptop (outside Docker) it is "localhost:9092".
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "vitals")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC_PATTERN = "wearables/{patient_id}/vitals"   # one MQTT topic per device
MQTT_SUBSCRIBE = "wearables/+/vitals"                    # "+" = any patient

# --------------------------------------------------------------------------
# 3. Alert thresholds (real-time threshold rules, PPT: "Analytics Layer")
# --------------------------------------------------------------------------
# An alert fires when ONE reading crosses one of these limits.
# "critical" limits decide the severity (CRITICAL vs WARNING).
ALERT_RULES = {
    "HIGH_HR":  {"label": "High heart rate (tachycardia)", "limit": 120,  "critical": 140},   # bpm, alert if >
    "LOW_SPO2": {"label": "Low SpO2 (hypoxia)",            "limit": 92,   "critical": 88},    # %,  alert if <
    "HIGH_BP":  {"label": "High blood pressure",           "sys_limit": 140, "dia_limit": 90,
                 "sys_critical": 180, "dia_critical": 120},                                  # mmHg, alert if >
    "FEVER":    {"label": "Fever",                         "limit": 38.0, "critical": 39.5},  # degC, alert if >
    "FALL":     {"label": "Fall detected",                 "critical": True},                 # always CRITICAL
}

# --------------------------------------------------------------------------
# 4. Data-quality rules (Spark "noise filter": readings outside these
#    physically possible ranges are treated as sensor errors and dropped)
# --------------------------------------------------------------------------
VALID_RANGES = {
    "heart_rate":   (30, 220),
    "spo2":         (70, 100),
    "systolic_bp":  (70, 250),
    "diastolic_bp": (40, 150),
    "body_temp":    (34.0, 42.0),
}

# --------------------------------------------------------------------------
# 5. Risk score used to LABEL training data for the MLlib model
# --------------------------------------------------------------------------
# Inspired by the NHS "National Early Warning Score" (NEWS2) idea: each vital
# sign that is outside its normal band adds points; more points = more risk.
# The points are computed on each patient's 1-minute averages (Gold layer).
#
#   Points  ->  Risk level
#   0 - 1   ->  Low     (label 0)
#   2 - 3   ->  Medium  (label 1)
#   4 +     ->  High    (label 2)
RISK_LEVELS = ["Low", "Medium", "High"]
RISK_MEDIUM_MIN_POINTS = 2
RISK_HIGH_MIN_POINTS = 4


def risk_points(avg_hr, avg_spo2, avg_sys, avg_dia, avg_temp, max_hr, min_spo2, falls):
    """
    Plain-Python version of the risk score (the Spark version in
    ml/risk_model.py uses exactly the same bands). Used by the unit tests.
    Returns the total number of points.
    """
    points = 0
    # Heart rate (average over the minute)
    if avg_hr > 110 or avg_hr < 45:
        points += 2
    elif avg_hr >= 95:
        points += 1
    # Oxygen saturation
    if avg_spo2 < 92:
        points += 2
    elif avg_spo2 < 95:
        points += 1
    # Blood pressure (worst of systolic / diastolic)
    if avg_sys > 140 or avg_dia > 90:
        points += 2
    elif avg_sys >= 130 or avg_dia >= 85:
        points += 1
    # Temperature
    if avg_temp > 38.0:
        points += 2
    elif avg_temp >= 37.3:
        points += 1
    # Short spikes inside the minute
    if max_hr > 120:
        points += 1
    if min_spo2 < 92:
        points += 1
    # A fall is serious on its own
    if falls > 0:
        points += 3
    return points


def risk_label(points):
    """Turn points into 0 = Low, 1 = Medium, 2 = High."""
    if points >= RISK_HIGH_MIN_POINTS:
        return 2
    if points >= RISK_MEDIUM_MIN_POINTS:
        return 1
    return 0
