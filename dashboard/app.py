"""
dashboard/app.py
----------------
PPT layer 5 - VISUALIZATION: live dashboard for doctors.

A Streamlit web page (http://localhost:8501) that reads the data lake
(Silver + Gold Parquet files written by Spark, and the MLlib results) and
refreshes itself every few seconds while the demo runs. It shows:

  * KPI cards       - patients monitored, readings ingested, avg alert latency,
                      critical alerts, high-risk patients, model accuracy
  * Live charts     - heart rate and SpO2 of one patient, with the alert threshold
  * Alerts by type  - bar chart          * Patient risk mix - donut chart (MLlib)
  * Red alert table - newest alerts first
  * Patient risk table, model details and a pipeline-status panel

Run (inside Docker):  docker compose up -d dashboard   ->  open http://localhost:8501
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import altair as alt
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import settings as S  # noqa: E402

# Replay mode (online demo, see dashboard/cloud_app.py): instead of the live data
# lake, replay a recording of real pipeline output from demo_data/ in a loop.
REPLAY = os.environ.get("DASHBOARD_MODE") == "replay"
REPLAY_DIR = os.path.join(S.PROJECT_ROOT, "demo_data")

# ---------------------------------------------------------------------------
# Colours: one accent for data lines/bars; status colours ONLY for risk/alerts
# (always paired with a text label, never colour alone).
# ---------------------------------------------------------------------------
ACCENT = "#4f9ee8"
GOOD, WARNING, CRITICAL = "#0ca30c", "#fab219", "#d03b3b"
RISK_COLORS = {"Low": GOOD, "Medium": WARNING, "High": CRITICAL}
RISK_ICONS = {"Low": "🟢 Low", "Medium": "🟠 Medium", "High": "🔴 High"}
ALERT_NAMES = {k: v["label"] for k, v in S.ALERT_RULES.items()}

st.set_page_config(page_title="Wearable Health Monitoring", page_icon="🩺", layout="wide")


# ===========================================================================
# 1. Reading the data lake
# ===========================================================================
def list_parquet_files(folder, newer_than_minutes=None):
    """All Parquet part-files under a layer folder (optionally only recent ones).
    Skips Spark's hidden bookkeeping folders (_spark_metadata, .crc files)."""
    cutoff = time.time() - newer_than_minutes * 60 if newer_than_minutes else 0
    found = []
    if not os.path.isdir(folder):
        return found
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if not d.startswith(("_", "."))]
        for name in files:
            if name.endswith(".parquet") and not name.startswith(("_", ".")):
                path = os.path.join(root, name)
                try:
                    if os.path.getmtime(path) >= cutoff:
                        found.append(path)
                except OSError:          # file vanished (e.g. ML overwrote it)
                    pass
    return sorted(found)


def read_parquet_files(paths):
    """Read many small Parquet files into one table. A file that Spark is still
    writing can't be read yet - we simply skip it and get it next refresh."""
    frames = []
    for path in paths:
        try:
            frames.append(pq.read_table(path).to_pandas())
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # Make every timestamp "plain UTC" (Spark may store them with or without a time zone).
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def safe_mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return None


@st.cache_resource
def _row_count_cache():
    """file path -> number of rows (Parquet files never change once written)."""
    return {}


def count_rows(folder):
    """Total rows in a layer, using each file's Parquet footer (fast, no data read)."""
    cache, total = _row_count_cache(), 0
    for path in list_parquet_files(folder):
        if path not in cache:
            try:
                cache[path] = pq.ParquetFile(path).metadata.num_rows
            except Exception:
                continue
        total += cache[path]
    return total


def load_patients():
    try:
        return pd.read_csv(os.path.join(REPLAY_DIR, "patients.csv") if REPLAY else S.PATIENTS_CSV)
    except Exception:
        return pd.DataFrame(columns=["patient_id", "age", "gender", "ward", "device_type", "device_id"])


def load_risk():
    """MLlib output. Kept in session memory so a refresh during an overwrite shows the last good copy."""
    paths = ([os.path.join(REPLAY_DIR, "patient_risk.parquet")] if REPLAY
             else list_parquet_files(S.GOLD_PATIENT_RISK))
    df = read_parquet_files(paths)
    if not df.empty:
        st.session_state["risk"] = df
    return st.session_state.get("risk", pd.DataFrame())


def load_metrics():
    try:
        with open(os.path.join(REPLAY_DIR, "model_metrics.json") if REPLAY else S.GOLD_MODEL_METRICS) as f:
            st.session_state["metrics"] = json.load(f)
    except Exception:
        pass
    return st.session_state.get("metrics", {})


@st.cache_data
def load_recording():
    """Replay mode: the recorded Silver readings, Gold alerts and recording info."""
    vitals = read_parquet_files([os.path.join(REPLAY_DIR, "vitals.parquet")])
    alerts = read_parquet_files([os.path.join(REPLAY_DIR, "alerts.parquet")])
    with open(os.path.join(REPLAY_DIR, "recording.json")) as f:
        info = json.load(f)
    return vitals, alerts, info


def replay_on_clock(df, start, length_sec, now):
    """Move recorded rows onto the current clock. The recording plays in a loop:
    the position is (seconds since 1970) modulo its length, so every viewer sees the
    same moment. Each row gets the time it was last "played"; rows not yet played in
    this loop come from the end of the previous loop."""
    position = now.timestamp() % length_sec
    offset = (df["event_time"] - start).dt.total_seconds()
    out = df.copy()
    out["event_time"] = now - pd.to_timedelta((position - offset) % length_sec, unit="s")
    return out


# ===========================================================================
# 2. Charts (Altair)
# ===========================================================================
def vital_chart(df, column, unit, threshold, above_is_bad, title):
    """Line chart of one vital sign with a dashed red alert-threshold line;
    readings that break the threshold are drawn as red dots."""
    base = alt.Chart(df).encode(x=alt.X("event_time:T", title="Time (UTC)"))
    line = base.mark_line(color=ACCENT, strokeWidth=2).encode(
        y=alt.Y(f"{column}:Q", title=unit, scale=alt.Scale(zero=False)))
    bad = (alt.datum[column] > threshold) if above_is_bad else (alt.datum[column] < threshold)
    alarm_dots = base.transform_filter(bad).mark_circle(color=CRITICAL, size=70, opacity=1).encode(
        y=f"{column}:Q")
    hover = base.mark_circle(size=90, opacity=0).encode(          # invisible points = hover tooltips
        y=f"{column}:Q",
        tooltip=[alt.Tooltip("event_time:T", title="time", format="%H:%M:%S"),
                 alt.Tooltip(f"{column}:Q", title=title)])
    limit = pd.DataFrame({"y": [threshold], "text": [f"Alert threshold {threshold} {unit}"]})
    rule = alt.Chart(limit).mark_rule(color=CRITICAL, strokeDash=[5, 4], strokeWidth=1.5).encode(y="y:Q")
    rule_text = alt.Chart(limit).mark_text(color=CRITICAL, align="left", dx=4, dy=-7, fontSize=11).encode(
        y="y:Q", x=alt.value(0), text="text:N")
    return (line + alarm_dots + hover + rule + rule_text).properties(height=260)


def alerts_by_type_chart(alerts):
    counts = alerts.groupby("alert_type").size().reset_index(name="alerts")
    counts["name"] = counts["alert_type"].map(ALERT_NAMES).fillna(counts["alert_type"])
    base = alt.Chart(counts).encode(
        y=alt.Y("name:N", sort="-x", title=None),
        x=alt.X("alerts:Q", title="Alerts in time window"))
    bars = base.mark_bar(color=ACCENT, cornerRadiusEnd=4, height=22).encode(
        tooltip=[alt.Tooltip("name:N", title="type"), alt.Tooltip("alerts:Q")])
    labels = base.mark_text(align="left", dx=4, color="#d7d7d4").encode(text="alerts:Q")
    return (bars + labels).properties(height=240)


def risk_donut(risk):
    counts = risk.groupby("risk_level").size().reset_index(name="patients")
    return alt.Chart(counts).mark_arc(innerRadius=55, stroke="#0e1117", strokeWidth=2).encode(
        theta=alt.Theta("patients:Q"),
        color=alt.Color("risk_level:N",
                        scale=alt.Scale(domain=list(RISK_COLORS), range=list(RISK_COLORS.values())),
                        legend=alt.Legend(title=None, orient="bottom")),
        tooltip=[alt.Tooltip("risk_level:N", title="risk"), alt.Tooltip("patients:Q")],
    ).properties(height=240)


def style_alert_rows(row):
    """Red table: CRITICAL rows dark red, WARNING rows lighter red."""
    colour = "#7a1f1f" if row["severity"] == "CRITICAL" else "#4a2323"
    return [f"background-color: {colour}; color: #ffffff"] * len(row)


# ===========================================================================
# 3. Page layout
# ===========================================================================
patients = load_patients()
patient_ids = sorted(patients["patient_id"].tolist())

with st.sidebar:
    st.header("⚙️ Controls")
    choice = st.selectbox("Patient for live charts",
                          ["Auto: latest alert"] + patient_ids, index=0,
                          help="'Auto' follows whichever patient raised the newest alert.")
    history_min = st.slider("History shown (minutes)", 2, 60, 10)
    refresh_sec = st.slider("Auto-refresh every (seconds)", 2, 30, 5)
    paused = st.toggle("Pause auto-refresh", value=False)
    st.caption("Data: Silver + Gold Parquet layers written by Spark Structured Streaming. "
               "Times are UTC.")

st.title("🩺 Wearable Health Monitoring – Live Dashboard")
st.caption("Wearables → Kafka → Spark Structured Streaming → Data lake (Bronze / Silver / Gold) "
           "→ Hive → Spark MLlib → this dashboard")
if REPLAY:
    _, _, _rec = load_recording()
    _minutes = (pd.Timestamp(_rec["recorded_to_utc"]) - pd.Timestamp(_rec["recorded_from_utc"])).total_seconds() / 60
    st.info(f"▶️ **Recorded replay.** This online copy plays back {_minutes:.0f} minutes of real output from the "
            f"pipeline (20 patients → Kafka → Spark → MLlib), recorded on {_rec['recorded_from_utc'][:10]}, "
            "in a loop. Run the project with Docker to see the live pipeline.")


@st.fragment(run_every=None if paused else refresh_sec)
def live_view():
    """Everything in here re-runs every `refresh_sec` seconds (only this part of the page)."""
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    if REPLAY:
        rec_vitals, rec_alerts, rec = load_recording()
        start = rec_vitals["event_time"].min()
        length = (rec_vitals["event_time"].max() - start).total_seconds() + 1
        silver = replay_on_clock(rec_vitals, start, length, now)
        alerts = replay_on_clock(rec_alerts, start, length, now)
    else:
        silver = read_parquet_files(list_parquet_files(S.SILVER_VITALS, newer_than_minutes=history_min + 2))
        alerts = read_parquet_files(list_parquet_files(S.GOLD_ALERTS, newer_than_minutes=history_min + 2))
    risk, metrics = load_risk(), load_metrics()

    if silver.empty:
        st.info("⏳ Waiting for data… Start the pipeline:  `docker compose up -d generator spark-streaming` "
                "– the first readings appear here about 30 seconds later.")
        return

    # keep only the chosen history window
    since = now - pd.Timedelta(minutes=history_min)
    silver = silver[silver["event_time"] >= since].sort_values("event_time")
    if silver.empty:
        st.info(f"⏳ No readings in the last {history_min} minutes – is the generator running?")
        return
    if not alerts.empty:
        alerts = alerts[alerts["event_time"] >= since].sort_values("event_time", ascending=False)

    # ------------------------- KPI cards -------------------------
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    active = silver[silver["event_time"] >= now - pd.Timedelta(minutes=1)]["patient_id"].nunique()
    k1.metric("Patients monitored", f"{active}", help="Patients that sent a reading in the last minute")
    ingested = (next(l["rows"] for l in rec["layers"] if l["layer"] == "Silver") if REPLAY
                else count_rows(S.SILVER_VITALS))
    k2.metric("Readings ingested", f"{ingested:,}", help="All clean readings in the Silver layer")
    latency = alerts["latency_sec"].mean() if not alerts.empty else None
    k3.metric("Avg alert latency", f"{latency:.1f} s" if latency is not None else "–",
              help="Device reading → alert written by Spark")
    critical = int((alerts["severity"] == "CRITICAL").sum()) if not alerts.empty else 0
    k4.metric("Critical alerts", f"{critical}", help=f"In the last {history_min} min")
    high = int((risk["risk_level"] == "High").sum()) if not risk.empty else None
    k5.metric("High-risk patients", f"{high}" if high is not None else "–", help="From the MLlib model")
    acc = metrics.get("accuracy")
    k6.metric("Risk model accuracy", f"{acc:.0%}" if acc is not None else "–", help="Decision tree, test set")

    # ------------------------- live vitals charts -------------------------
    if choice.startswith("Auto"):
        patient = alerts.iloc[0]["patient_id"] if not alerts.empty else silver.iloc[-1]["patient_id"]
    else:
        patient = choice
    one = silver[silver["patient_id"] == patient]
    info = patients[patients["patient_id"] == patient]
    who = f"{patient}" + (f" · {info.iloc[0]['ward']} · age {info.iloc[0]['age']}" if not info.empty else "")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"Heart rate – {who}")
        st.altair_chart(vital_chart(one, "heart_rate", "bpm", S.ALERT_RULES["HIGH_HR"]["limit"],
                                    True, "heart rate"), use_container_width=True)
    with c2:
        st.subheader(f"SpO2 – {who}")
        st.altair_chart(vital_chart(one, "spo2", "%", S.ALERT_RULES["LOW_SPO2"]["limit"],
                                    False, "SpO2"), use_container_width=True)

    # ------------------------- alerts by type + risk donut -------------------------
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Alerts by type")
        if alerts.empty:
            st.caption("No alerts in this time window.")
        else:
            st.altair_chart(alerts_by_type_chart(alerts), use_container_width=True)
    with c4:
        st.subheader("Patient risk level (MLlib)")
        if risk.empty:
            st.caption("No predictions yet – start the model: `docker compose up -d ml`")
        else:
            st.altair_chart(risk_donut(risk), use_container_width=True)

    # ------------------------- red alerts table -------------------------
    st.subheader("🚨 Latest alerts")
    if alerts.empty:
        st.caption("No alerts in this time window.")
    else:
        table = alerts.head(15)[["event_time", "patient_id", "alert_type", "severity", "message", "latency_sec"]].copy()
        table["event_time"] = table["event_time"].dt.strftime("%H:%M:%S")
        table = table.rename(columns={"event_time": "time (UTC)", "patient_id": "patient",
                                      "alert_type": "type", "latency_sec": "latency (s)"})
        st.dataframe(table.style.apply(style_alert_rows, axis=1).format({"latency (s)": "{:.1f}"}),
                     use_container_width=True, hide_index=True)

    # ------------------------- patient risk table -------------------------
    st.subheader("Patient risk levels")
    if risk.empty:
        st.caption("Waiting for the MLlib model (`docker compose up -d ml`).")
    else:
        view = risk.merge(patients[["patient_id", "age", "ward"]], on="patient_id", how="left")
        view["risk"] = view["risk_level"].map(RISK_ICONS)
        view = view.sort_values("risk_score", ascending=False)[
            ["patient_id", "risk", "risk_score", "cluster_group", "age", "ward",
             "avg_hr", "avg_spo2", "avg_sys", "avg_dia", "avg_temp"]]
        st.dataframe(view, use_container_width=True, hide_index=True, column_config={
            "patient_id": "patient",
            "risk_score": st.column_config.ProgressColumn("risk score", min_value=0, max_value=100, format="%.0f"),
            "cluster_group": "cluster (KMeans)",
            "avg_hr": "HR", "avg_spo2": "SpO2", "avg_sys": "sys BP", "avg_dia": "dia BP", "avg_temp": "temp °C",
        })
        if metrics.get("trained_at_utc"):
            st.caption(f"Model last run {metrics['trained_at_utc']} UTC"
                       + (" (recorded)" if REPLAY else " · re-scored every 60 s"))

    # ------------------------- details for the viva -------------------------
    with st.expander("🌳 How the MLlib model decides (decision tree rules)"):
        if metrics:
            st.write(f"**{metrics.get('model')}** · test accuracy **{metrics.get('accuracy', 0):.1%}** · "
                     f"F1 **{metrics.get('f1', 0):.2f}** · trained on {metrics.get('training_windows')} "
                     f"one-minute windows")
            st.write("Feature importance:", metrics.get("feature_importances"))
            st.code(metrics.get("tree_rules", ""), language="text")
        else:
            st.caption("Model not trained yet.")

    with st.expander("🗂️ Pipeline status (data lake layers)"):
        if REPLAY:
            st.caption(f"Data lake as recorded at {rec['recorded_to_utc'][:19]} UTC.")
            rows = rec["layers"]
        else:
            rows = []
            for layer, name, folder in [("Bronze", "raw_vitals", S.BRONZE_RAW),
                                        ("Silver", "vitals", S.SILVER_VITALS),
                                        ("Gold", "alerts", S.GOLD_ALERTS),
                                        ("Gold", "vitals_1min", S.GOLD_VITALS_1MIN),
                                        ("Gold", "patient_risk", S.GOLD_PATIENT_RISK)]:
                files = list_parquet_files(folder)
                newest = max((t for t in map(safe_mtime, files) if t), default=None)
                rows.append({"layer": layer, "table": name, "parquet files": len(files),
                             "rows": count_rows(folder),
                             "last write (UTC)": datetime.fromtimestamp(newest, timezone.utc).strftime("%H:%M:%S")
                             if newest else "–"})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.caption(f"Last refresh {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")


live_view()
