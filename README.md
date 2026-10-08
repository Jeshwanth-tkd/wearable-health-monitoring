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
| 3 | Wearable data simulator | ⏳ next |
| 4 | Spark Structured Streaming + Bronze/Silver/Gold | ⏳ |
| 5 | Hive queries | ⏳ |
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
