# MediStream Telehealth Analytics Platform
## ISM 6562 — Final Project

End-to-end data platform for the MediStream telehealth scenario: HDFS data lake, Spark batch transforms, Kafka streaming for real-time alerts, and Airflow orchestration.

## Team
- Stage 1: HDFS Data Lake — @sana-afroze
- Stage 2: Spark Batch Transformation — @svela
- Stage 3: Kafka Streaming — @Harshxth
- Stage 4: Airflow Orchestration — @SheikhRobinEmon-USF

## Prerequisites

- Docker Desktop running (16 GB RAM recommended)
- Windows users: use WSL2 terminal, and run `git config --global core.autocrlf false` before cloning

## Setup

### 1. Clone

```bash
git clone https://github.com/sana-afroze/MediStream-Telehealth.git
cd MediStream-Telehealth
```

### 2. Add the data files

Download these 5 files from the [course data repo](https://github.com/prof-tcsmith/ism6562s26-class/tree/main/final-projects/data/04-medistream-telehealth/) and put them in `data/`:

```
appointments.csv.gz
patient-vitals.json.gz
session-quality.csv.gz
patient-feedback.json.gz
physician-schedule.csv.gz
```

### 3. Start the stack

First time (builds the custom Spark and Airflow images):

```bash
docker compose up -d --build
```

Subsequent runs:

```bash
docker compose up -d
```

Wait ~60s for HDFS, ~30s for Spark, ~20s for Kafka, ~60s for Airflow.

## Stage 1 — HDFS

Create zones and load raw data:

```bash
docker exec -it hadoop-namenode bash /hdfs-init/create-zones.sh

docker exec -it hadoop-namenode bash -c "
hdfs dfs -put -f /data/appointments.csv.gz       /medistream/landing/appointments/
hdfs dfs -put -f /data/patient-vitals.json.gz    /medistream/landing/patient_vitals/
hdfs dfs -put -f /data/session-quality.csv.gz    /medistream/landing/session_quality/
hdfs dfs -put -f /data/patient-feedback.json.gz  /medistream/landing/patient_feedback/
hdfs dfs -put -f /data/physician-schedule.csv.gz /medistream/landing/physician_schedule/
"
```

Browse HDFS at http://localhost:9870 — files should appear in `/medistream/landing/`.

## Stage 2 — Spark Batch Transforms

Open Jupyter at http://localhost:8888?token=spark and run notebooks in order:

1. `02-spark-transforms.ipynb` — main pipeline: clean + curate the 5 datasets, build 5 analytics tables.
2. `02b-no-show-breakdown.ipynb` — no-show rates by specialty × time-of-day × day-of-week × visit_type.
3. `02c-quality-by-device-os.ipynb` — session quality by device_type and OS.
4. `02d-derived-features.ipynb` — patient history score + physician quality-adjusted volume.
5. `02e-degraded-sessions.ipynb` — flag sessions where call quality dropped below thresholds.
6. `02f-repartition-curated.ipynb` — partition curated Parquet by specialty / device_type / measurement_type.
7. `02g-followup-health-check.ipynb` — verify all curated and analytics tables.

## Stage 3 — Kafka Streaming

Create the topics:

```bash
docker exec -it medistream-kafka bash /kafka-init/create-topics.sh
```

Run the streaming consumer first, then the producer:

1. `03b-streaming-consumer.ipynb` — Spark Structured Streaming. The last cell calls `awaitTermination()` and blocks while the query runs.
2. In a separate Jupyter tab, `03a-streaming-producer.ipynb` — replays `session-quality.csv.gz` as a Kafka stream.
3. `03c-streaming-health-check.ipynb` — confirm the topics, offsets, and Parquet outputs.

Alerts land on Kafka topic `session-alerts` and at HDFS `/medistream/analytics/streaming_alerts/` partitioned by `alert_date`.

## Stage 4 — Airflow Orchestration

Open Airflow at http://localhost:8090 — login `admin` / `admin`.

Two DAGs:

- **`medistream_batch_pipeline`** (daily) — checks landing zone → quality gate → Spark transform marker → verifies curated outputs. Real HDFS WebHDFS calls; fails with a useful error if any stage hasn't run.
- **`medistream_streaming_health_monitor`** (hourly) — pings Kafka, reports topic offsets for `session-metrics` and `session-alerts`, verifies Spark master is reachable.

Unpause both DAGs and trigger manually to verify.

## Service URLs

| Service | URL |
|---|---|
| HDFS UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Spark App UI | http://localhost:4040 (active during a job) |
| Jupyter | http://localhost:8888?token=spark |
| Airflow UI | http://localhost:8090 |
| Kafka | localhost:9092 (host) / kafka:9093 (compose network) |

## Project Structure

```
MediStream-Telehealth/
├── data/                                  ← data files (not committed to git)
├── dags/
│   ├── batch_pipeline.py                  ← Stage 4: daily batch DAG
│   └── streaming_monitor.py               ← Stage 4: hourly streaming health DAG
├── docker/
│   ├── Dockerfile.spark                   ← Spark image
│   └── Dockerfile.airflow                 ← Airflow image
├── hdfs-init/
│   └── create-zones.sh                    ← HDFS zone creation
├── kafka-init/
│   └── create-topics.sh                   ← Kafka topic creation
├── notebooks/
│   ├── 02-spark-transforms.ipynb          ← Stage 2 main pipeline
│   ├── 02b-no-show-breakdown.ipynb
│   ├── 02c-quality-by-device-os.ipynb
│   ├── 02d-derived-features.ipynb
│   ├── 02e-degraded-sessions.ipynb
│   ├── 02f-repartition-curated.ipynb
│   ├── 02g-followup-health-check.ipynb
│   ├── 03a-streaming-producer.ipynb       ← Stage 3 producer
│   ├── 03b-streaming-consumer.ipynb       ← Stage 3 consumer
│   └── 03c-streaming-health-check.ipynb   ← Stage 3 health check
├── docker-compose.yml
├── hadoop.env
├── .gitattributes
└── .gitignore
```
