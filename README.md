# Healthcare – Wearable Device Health Monitoring

A working Big Data mini-project that follows the architecture in our slides:

```
Wearables → (MQTT) → Kafka → Spark Structured Streaming → Data Lake Bronze/Silver/Gold → Hive → Spark MLlib → Dashboard
```

## Project status

| Phase | What | Status |
|---|---|---|
| 1 | Project setup (folders, requirements, .gitignore) | ✅ done |
| 2 | Kafka with Docker Compose | ⏳ next |
| 3 | Wearable data simulator | ⏳ |
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

## How to run (so far)

Nothing to run yet – Phase 1 only creates the project skeleton.
