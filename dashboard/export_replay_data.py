"""
dashboard/export_replay_data.py
-------------------------------
Saves a RECORDING of the real pipeline output into demo_data/, so the dashboard
can be hosted online (Streamlit Community Cloud) without Kafka or Spark.
dashboard/cloud_app.py replays this recording in a loop.

Run while (or after) the pipeline has been running for 10+ minutes:
    docker compose exec dashboard python3 dashboard/export_replay_data.py

It keeps the longest stretch of readings with no gap (max 30 minutes), plus
the alerts in that stretch, the latest MLlib results and the patient list.
"""

import json
import os
import shutil
import sys

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings as S  # noqa: E402

OUT_DIR = os.path.join(S.PROJECT_ROOT, "demo_data")
MAX_MINUTES = 30
GAP_SECONDS = 20          # a pause longer than this = the pipeline was stopped


def parquet_files(folder):
    found = []
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if not d.startswith(("_", "."))]
        found += [os.path.join(root, f) for f in files
                  if f.endswith(".parquet") and not f.startswith(("_", "."))]
    return sorted(found)


def read_layer(folder):
    frames = [pq.read_table(p).to_pandas() for p in parquet_files(folder)]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return df.drop(columns=["event_date"], errors="ignore")


def main():
    vitals = read_layer(S.SILVER_VITALS).sort_values("event_time").reset_index(drop=True)
    if vitals.empty:
        sys.exit("No Silver data yet - run the pipeline first.")

    # Longest stretch without a gap, so the replay never shows a hole.
    segment = (vitals["event_time"].diff().dt.total_seconds() > GAP_SECONDS).cumsum()
    lengths = vitals.groupby(segment)["event_time"].agg(lambda t: t.max() - t.min())
    best = vitals[segment == lengths.idxmax()]
    start = best["event_time"].max() - pd.Timedelta(minutes=MAX_MINUTES)
    best = best[best["event_time"] >= start]
    t0, t1 = best["event_time"].min(), best["event_time"].max()

    alerts = read_layer(S.GOLD_ALERTS)
    alerts = alerts[(alerts["event_time"] >= t0) & (alerts["event_time"] <= t1)]
    risk = read_layer(S.GOLD_PATIENT_RISK)

    os.makedirs(OUT_DIR, exist_ok=True)
    best.to_parquet(os.path.join(OUT_DIR, "vitals.parquet"), index=False)
    alerts.to_parquet(os.path.join(OUT_DIR, "alerts.parquet"), index=False)
    risk.to_parquet(os.path.join(OUT_DIR, "patient_risk.parquet"), index=False)
    shutil.copy(S.GOLD_MODEL_METRICS, os.path.join(OUT_DIR, "model_metrics.json"))
    shutil.copy(S.PATIENTS_CSV, os.path.join(OUT_DIR, "patients.csv"))

    layers = [("Bronze", "raw_vitals", S.BRONZE_RAW), ("Silver", "vitals", S.SILVER_VITALS),
              ("Gold", "alerts", S.GOLD_ALERTS), ("Gold", "vitals_1min", S.GOLD_VITALS_1MIN),
              ("Gold", "patient_risk", S.GOLD_PATIENT_RISK)]
    info = {
        "recorded_from_utc": str(t0), "recorded_to_utc": str(t1),
        "layers": [{"layer": layer, "table": name, "parquet files": len(parquet_files(folder)),
                    "rows": sum(pq.ParquetFile(p).metadata.num_rows for p in parquet_files(folder))}
                   for layer, name, folder in layers],
    }
    with open(os.path.join(OUT_DIR, "recording.json"), "w") as f:
        json.dump(info, f, indent=2)

    print(f"Saved {len(best)} readings and {len(alerts)} alerts "
          f"({(t1 - t0).total_seconds() / 60:.1f} min, {t0} -> {t1} UTC) to {OUT_DIR}")


if __name__ == "__main__":
    main()
