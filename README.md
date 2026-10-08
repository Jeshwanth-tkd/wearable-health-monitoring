# Healthcare – Wearable Device Health Monitoring

A working Big Data mini-project that follows the architecture in our slides:

```
Wearables → (MQTT) → Kafka → Spark Structured Streaming → Data Lake Bronze/Silver/Gold → Hive → Spark MLlib → Dashboard
```

## Project status

| Phase | What | Status |
|---|---|---|
| 1 | Project setup (folders, requirements, .gitignore) | ✅ done |
| 2 | Kafka with Docker Compose | ✅ done |
| 3 | Wearable data simulator | ✅ done |
| 4 | Spark Structured Streaming + Bronze/Silver/Gold | ✅ done |
| 5 | Hive queries | ⏳ next |
| 6 | Spark MLlib risk model | ⏳ |
| 7 | Streamlit dashboard | ⏳ |
| 8 | Final docs + viva questions | ⏳ |

## Folder structure

```
wearable-health-monitoring/
├── common/settings.py        shared paths, Kafka topic, alert thresholds, risk-score rules
├── generator/                wearable device simulator (Phase 3)
├── ingestion/                optional MQTT broker config + MQTT → Kafka bridge (Phase 3)
├── spark/                    Spark Structured Streaming job (Phase 4)
├── hive/                     Hive table definitions + example HiveQL queries (Phase 5)
├── ml/                       Spark MLlib risk model (Phase 6)
├── dashboard/                Streamlit live dashboard (Phase 7)
├── docs/                     how-it-works notes, viva questions, screenshots
├── tests/                    small logic tests (no Kafka/Spark needed)
├── data/                     the data lake: bronze/ silver/ gold/ (created at runtime, not committed)
├── requirements.txt          pinned Python libraries
└── .gitignore                keeps data, checkpoints, venvs and secrets out of git
```

## Before you start (one-time setup)

You only need **two programs** on your laptop. Everything else (Java 17, Spark 3.5.1, Kafka, Python libraries) runs inside Docker, so the steps are the same on Windows and Mac.

1. **Docker Desktop** – https://www.docker.com/products/docker-desktop/
   - Windows: during install keep "Use WSL 2" ticked. Restart when asked.
   - Mac: pick the Apple-chip or Intel download that matches your Mac.
   - Open Docker Desktop and wait until it says **"Engine running"**.
   - RAM: *Settings → Resources* (Windows with WSL 2 manages this automatically). Give Docker **at least 4 GB** – 6 GB is comfortable on a 16 GB laptop.
2. **Git** – https://git-scm.com/downloads (Mac: run `git --version` once and accept the install prompt).

Open a terminal (**Windows: PowerShell**, **Mac: Terminal**) and go into the project folder:

```bash
cd wearable-health-monitoring
```

All commands below are typed in that folder. They are identical on Windows and Mac.

## How to run (so far)

### Phase 2 – Start Kafka

```bash
docker compose up -d kafka kafka-init
```

The first time, this downloads the Kafka image (~400 MB). Check it worked:

```bash
docker compose ps                 # "kafka" should show (healthy)
docker compose logs kafka-init    # should print: Topic: vitals  PartitionCount: 3
```

List the topics yourself:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Expected output: `vitals`

Stop everything (keeps the data) with `docker compose stop`; start again with `docker compose start`.

### Phase 3 – Start the wearable simulator

Builds our app image (first time only, ~5–10 min: it downloads Spark 3.5.1 + Java 17 + Python libraries), then starts streaming 20 patients into Kafka:

```bash
docker compose up -d --build generator
docker compose logs -f generator        # Ctrl+C to stop watching (the generator keeps running)
```

You should see lines like:

```
[simulator] connected to Kafka at kafka:29092, topic 'vitals'
[simulator] streaming 20 patients every 1-2 s. Press Ctrl+C to stop.
[simulator] !! P-017: abnormal episode started -> tachycardia
[simulator] sent 1320 readings so far (3 abnormal episodes, 5 invalid)
```

Read 5 messages straight from the Kafka topic to prove they arrived:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic vitals --max-messages 5
```

Each message looks like:

```json
{"patient_id": "P-007", "device_id": "DEV-007", "timestamp": "2026-10-08 04:02:05.496", "heart_rate": 94, "spo2": 94.2,
 "systolic_bp": 132, "diastolic_bp": 83, "body_temp": 37.2, "steps": 1757, "fall_detected": false}
```

Run the logic tests (no Kafka needed):

```bash
docker compose run --rm generator python3 -m unittest tests/test_logic.py -v
```

**Optional – full MQTT path from the PPT** (smartphone → MQTT broker → Kafka). Use this *instead of* the normal generator:

```bash
docker compose stop generator
docker compose --profile mqtt up -d mosquitto mqtt-bridge generator-mqtt
docker compose logs -f mqtt-bridge      # "forwarded 500 messages MQTT -> Kafka"
```

### Phase 4 – Start the Spark streaming job (Bronze / Silver / Gold)

> **Storage note:** HDFS in Docker needs a NameNode + DataNode (~2 GB extra RAM) and often breaks on laptops because the DataNode's hostname isn't reachable from outside Docker. For reliability **we use local folders `data/bronze`, `data/silver`, `data/gold` instead of HDFS.** The layout, Parquet format and date partitioning are the same as in the PPT. Spark writes through the Hadoop FileSystem API either way, so on a real cluster only the path changes (`hdfs://...`).

```bash
docker compose up -d spark-streaming
docker compose logs -f spark-streaming
```

After ~30 seconds you'll see a status line every 30 s:

```
[streaming] bronze_raw_vitals  batch=12     rows_in_last_batch=131    status=Waiting for next trigger
[streaming] silver_vitals      batch=25     rows_in_last_batch=66     status=Waiting for next trigger
[streaming] gold_alerts        batch=41     rows_in_last_batch=39     status=Waiting for next trigger
[streaming] gold_vitals_1min   batch=12     rows_in_last_batch=131    status=Waiting for next trigger
```

Folders now appear in your project's `data/` folder:

```
data/bronze/raw_vitals/ingest_date=2026-10-08/part-....parquet   raw JSON from Kafka
data/silver/vitals/event_date=2026-10-08/...                     cleaned readings
data/gold/alerts/event_date=2026-10-08/...                       threshold alerts
data/gold/vitals_1min/event_date=2026-10-08/...                  1-minute averages per patient
data/checkpoints/...                                             Spark's progress bookmarks
```

Check every layer (counts, sample rows, alerts by type):

```bash
docker compose exec spark-streaming spark-submit spark/check_layers.py
```

The 1-minute windows appear about **2 minutes** after start. That's because a window is only written once it is closed: its minute must end, plus the 1-minute watermark for late readings.

Spark UI: open http://localhost:4040 → **Structured Streaming** tab to see input rate and batch durations (nice for the viva).
