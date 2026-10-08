"""
ml/risk_model.py
----------------
PPT layer 4 - ANALYTICS: Spark MLlib health-risk prediction (+ patient clustering).

INPUT : Gold layer  data/gold/vitals_1min   (1-minute averages per patient)
OUTPUT: Gold layer  data/gold/patient_risk  (Low / Medium / High per patient)
        data/gold/model_metrics.json        (accuracy, tree rules - shown on the dashboard)
        data/models/risk_decision_tree      (the saved MLlib model)

HOW IT WORKS (simple and explainable):

 1. LABELS - we have no doctor-labelled data, so each 1-minute window gets a
    label from an early-warning score (like the NHS NEWS2 score): every vital
    sign outside its normal band adds points.
        0-1 points = Low (0),  2-3 = Medium (1),  4+ = High (2)
    (bands are in common/settings.py -> risk_points)

 2. MODEL - a Spark MLlib DecisionTreeClassifier (max depth 4) learns to
    predict the label from the window's features:
        avg_hr, max_hr, avg_spo2, min_spo2, avg_sys, avg_dia, avg_temp, falls
    A decision tree is a set of IF/ELSE rules, so we can print it and explain
    every prediction. 80% of windows train it, 20% test it (accuracy, F1).

 3. PATIENT RISK - each patient's level = majority vote of the model's
    predictions for their last 5 minutes (so one noisy minute doesn't flip it).
    risk_score (0-100) = average of P(Medium)x50 + P(High)x100.

 4. CLUSTERING - KMeans (k=3) groups patients with similar average vitals
    (the PPT's "patient clustering"): Stable / Watch / Unstable.

Run once:          docker compose run --rm ml-once
Keep re-scoring:   docker compose up -d ml          (re-runs every 60 s for the live dashboard)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings as S  # noqa: E402

FEATURES = ["avg_hr", "max_hr", "avg_spo2", "min_spo2", "avg_sys", "avg_dia", "avg_temp", "falls"]
RECENT_WINDOWS = 5          # majority vote over each patient's last 5 minutes
MIN_WINDOWS_TO_TRAIN = 40   # wait until there is enough Gold data


def build_spark():
    spark = (SparkSession.builder.appName("WearableHealth-MLlib-Risk")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def add_rule_label(df):
    """Spark version of settings.risk_points(): points -> label 0/1/2 (Low/Medium/High)."""
    hr, spo2 = F.col("avg_hr"), F.col("avg_spo2")
    sbp, dbp, temp = F.col("avg_sys"), F.col("avg_dia"), F.col("avg_temp")
    points = (
        F.when((hr > 110) | (hr < 45), 2).when(hr >= 95, 1).otherwise(0)
        + F.when(spo2 < 92, 2).when(spo2 < 95, 1).otherwise(0)
        + F.when((sbp > 140) | (dbp > 90), 2).when((sbp >= 130) | (dbp >= 85), 1).otherwise(0)
        + F.when(temp > 38.0, 2).when(temp >= 37.3, 1).otherwise(0)
        + F.when(F.col("max_hr") > 120, 1).otherwise(0)
        + F.when(F.col("min_spo2") < 92, 1).otherwise(0)
        + F.when(F.col("falls") > 0, 3).otherwise(0)
    )
    return (df.withColumn("rule_points", points)
              .withColumn("label",
                          F.when(F.col("rule_points") >= S.RISK_HIGH_MIN_POINTS, 2.0)
                           .when(F.col("rule_points") >= S.RISK_MEDIUM_MIN_POINTS, 1.0)
                           .otherwise(0.0)))


def readable_tree(tree_model):
    """Replace 'feature 0' etc. with real column names so the tree reads like IF/ELSE rules."""
    text = tree_model.toDebugString
    for i, name in enumerate(FEATURES):
        text = text.replace(f"feature {i} ", f"{name} ")
    for i, level in enumerate(S.RISK_LEVELS):
        text = text.replace(f"Predict: {float(i)}", f"Predict: {level}")
    return text


def write_json_atomic(path, data):
    """Write JSON to a temp file then rename, so the dashboard never reads half a file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, path)


def run_once(spark):
    """Train, evaluate, score every patient, save. Returns False if there is not enough data yet."""
    if not os.path.exists(S.GOLD_VITALS_1MIN):
        print("[ml] no Gold 1-minute windows yet - is the streaming job running?")
        return False
    windows = spark.read.parquet(S.GOLD_VITALS_1MIN).dropna(subset=FEATURES)
    n = windows.count()
    if n < MIN_WINDOWS_TO_TRAIN:
        print(f"[ml] only {n} one-minute windows so far (need {MIN_WINDOWS_TO_TRAIN}) - waiting for more data")
        return False

    data = add_rule_label(windows).cache()
    label_counts = {S.RISK_LEVELS[int(r["label"])]: r["count"]
                    for r in data.groupBy("label").count().collect()}

    # ---------------- 1. train + evaluate the decision tree ----------------
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    tree = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=4, seed=42)
    pipeline = Pipeline(stages=[assembler, tree])

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    if test.count() == 0:
        test = train
    model = pipeline.fit(train)
    tested = model.transform(test)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy = evaluator.setMetricName("accuracy").evaluate(tested)
    f1 = evaluator.setMetricName("f1").evaluate(tested)
    model.write().overwrite().save(S.MODEL_DIR)

    tree_model = model.stages[-1]
    importances = {FEATURES[i]: round(float(v), 3) for i, v in enumerate(tree_model.featureImportances.toArray())}

    # ---------------- 2. score every window, then each patient ----------------
    scored = (model.transform(data)
              .withColumn("p", vector_to_array("probability"))
              .withColumn("window_score",
                          F.coalesce(F.col("p").getItem(1), F.lit(0.0)) * 50
                          + F.coalesce(F.col("p").getItem(2), F.lit(0.0)) * 100))

    latest_first = Window.partitionBy("patient_id").orderBy(F.col("window_start").desc())
    recent = scored.withColumn("rn", F.row_number().over(latest_first)).filter(F.col("rn") <= RECENT_WINDOWS)

    # majority vote of the recent predictions (a tie goes to the HIGHER risk)
    votes = recent.groupBy("patient_id", "prediction").count()
    vote_order = Window.partitionBy("patient_id").orderBy(F.col("count").desc(), F.col("prediction").desc())
    winner = (votes.withColumn("r", F.row_number().over(vote_order)).filter(F.col("r") == 1)
              .select("patient_id", F.col("prediction").alias("risk_label")))

    summary = recent.groupBy("patient_id").agg(
        F.round(F.avg("window_score"), 1).alias("risk_score"),
        F.count(F.lit(1)).alias("windows_used"),
        F.max("window_end").alias("last_window_end"),
        F.round(F.avg("avg_hr"), 1).alias("avg_hr"),
        F.round(F.avg("avg_spo2"), 2).alias("avg_spo2"),
        F.round(F.avg("avg_sys"), 1).alias("avg_sys"),
        F.round(F.avg("avg_dia"), 1).alias("avg_dia"),
        F.round(F.avg("avg_temp"), 2).alias("avg_temp"),
        F.max("rule_points").alias("max_rule_points"),
    )
    level_names = F.element_at(F.array(*[F.lit(x) for x in S.RISK_LEVELS]), F.col("risk_label").cast("int") + 1)
    patient_risk = (summary.join(winner, "patient_id")
                    .withColumn("risk_level", level_names))

    # ---------------- 3. KMeans patient clustering ----------------
    per_patient = data.groupBy("patient_id").agg(
        F.avg("avg_hr").alias("c_hr"), F.avg("avg_spo2").alias("c_spo2"),
        F.avg("avg_sys").alias("c_sys"), F.avg("avg_temp").alias("c_temp"),
        (F.sum("abnormal_readings") / F.sum("readings")).alias("c_abnormal_rate"))
    n_patients = per_patient.count()
    if n_patients >= 3:
        cluster_cols = ["c_hr", "c_spo2", "c_sys", "c_temp", "c_abnormal_rate"]
        km_pipeline = Pipeline(stages=[
            VectorAssembler(inputCols=cluster_cols, outputCol="raw_features"),
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
            KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster"),
        ])
        clustered = km_pipeline.fit(per_patient).transform(per_patient)
        # Name the clusters by how unwell their average patient looks (more risk points = less stable).
        centers = clustered.groupBy("cluster").agg(*[F.avg(c).alias(c) for c in cluster_cols]).collect()
        ranked = sorted(centers, key=lambda c: (
            S.risk_points(c["c_hr"], c["c_spo2"], c["c_sys"], 80, c["c_temp"], c["c_hr"], c["c_spo2"], 0),
            c["c_abnormal_rate"]))
        names = {row["cluster"]: name for row, name in zip(ranked, ["Stable", "Watch", "Unstable"])}
        name_map = F.create_map(*[x for cid, nm in names.items() for x in (F.lit(cid), F.lit(nm))])
        clusters = clustered.select("patient_id", "cluster", name_map[F.col("cluster")].alias("cluster_group"))
        patient_risk = patient_risk.join(clusters, "patient_id", "left")
    else:
        patient_risk = (patient_risk.withColumn("cluster", F.lit(None).cast("int"))
                        .withColumn("cluster_group", F.lit(None).cast("string")))

    scored_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    patient_risk = (patient_risk
                    .withColumn("scored_at", F.to_timestamp(F.lit(scored_at)))
                    .select("patient_id", "risk_level", "risk_score", "cluster", "cluster_group",
                            "avg_hr", "avg_spo2", "avg_sys", "avg_dia", "avg_temp",
                            "max_rule_points", "windows_used", "last_window_end", "scored_at")
                    .orderBy(F.col("risk_score").desc()))

    # ---------------- 4. save results ----------------
    patient_risk.coalesce(1).write.mode("overwrite").parquet(S.GOLD_PATIENT_RISK)
    risk_counts = {r["risk_level"]: r["count"] for r in patient_risk.groupBy("risk_level").count().collect()}
    write_json_atomic(S.GOLD_MODEL_METRICS, {
        "trained_at_utc": scored_at,
        "model": "Spark MLlib DecisionTreeClassifier (maxDepth=4)",
        "features": FEATURES,
        "training_windows": train.count(),
        "test_windows": test.count(),
        "accuracy": round(accuracy, 4),
        "f1": round(f1, 4),
        "label_distribution": label_counts,
        "feature_importances": importances,
        "patient_risk_counts": risk_counts,
        "tree_rules": readable_tree(tree_model),
    })
    data.unpersist()

    # ---------------- 5. print an explainable report ----------------
    print("\n==================== MLlib RISK MODEL ====================")
    print(f"windows used: {n}   label mix: {label_counts}")
    print(f"test accuracy: {accuracy:.3f}   F1: {f1:.3f}")
    print("feature importance:", dict(sorted(importances.items(), key=lambda kv: -kv[1])))
    print("\nDecision tree (IF/ELSE rules the model learned):")
    print(readable_tree(tree_model))
    print("Patient risk levels:")
    patient_risk.select("patient_id", "risk_level", "risk_score", "cluster_group",
                        "avg_hr", "avg_spo2", "avg_sys", "avg_temp").show(30, truncate=False)
    print(f"saved -> {S.GOLD_PATIENT_RISK}  and  {S.GOLD_MODEL_METRICS}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Train the MLlib risk model and score patients")
    parser.add_argument("--every", type=int, default=0,
                        help="repeat every N seconds (0 = run once and exit)")
    args = parser.parse_args()

    spark = build_spark()
    try:
        while True:
            try:
                done = run_once(spark)
            except Exception as exc:      # keep the loop alive during a live demo
                print(f"[ml] this round failed: {exc.__class__.__name__}: {str(exc)[:300]}")
                done = False
            if not args.every:
                if not done:
                    print("[ml] not enough data yet - let the generator + streaming job run a few minutes, then retry.")
                break
            time.sleep(args.every if done else 30)
    except KeyboardInterrupt:
        pass
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
