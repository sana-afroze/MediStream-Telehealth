# MediStream Telehealth Analytics Platform
## ISM 6562 — Final Project

## Team
- Stage 1: HDFS Data Lake — @sana-afroze
- Stage 2: Spark Batch Transformation — @svela
- Stage 3: Kafka Streaming — @Harshxth
- Stage 4: Airflow Orchestration — @SheikhRobinEmon *(Batch Pipeline & Streaming Monitor)*

## Quick Start

### Prerequisites
- Docker Desktop running (16 GB RAM recommended)
- Data files (get from course data repo)

### 1. Clone the repo
```bash
git clone https://github.com/sana-afroze/MediStream-Telehealth.git
cd MediStream-Telehealth
```

### 2. Add data files
Place these 5 files into the `data/` folder:
```
appointments.csv.gz
patient-vitals.json.gz
session-quality.csv.gz
patient-feedback.json.gz
physician-schedule.csv.gz
```
Download from: `https://github.com/prof-tcsmith/ism6562s26-class/tree/main/final-projects/data/04-medistream-telehealth/`

### 3. Start the full stack
```bash
docker compose up -d
```
Wait ~60s for HDFS, ~30s for Spark, ~20s for Kafka.

### 4. Create HDFS zones
```bash
docker exec -it hadoop-namenode bash /hdfs-init/create-zones.sh
```

### 5. Load data into HDFS
```bash
docker exec -it hadoop-namenode bash -c "
hdfs dfs -put /data/appointments.csv.gz /medistream/landing/appointments/
hdfs dfs -put /data/patient-vitals.json.gz /medistream/landing/patient_vitals/
hdfs dfs -put /data/session-quality.csv.gz /medistream/landing/session_quality/
hdfs dfs -put /data/patient-feedback.json.gz /medistream/landing/patient_feedback/
hdfs dfs -put /data/physician-schedule.csv.gz /medistream/landing/physician_schedule/
"
```

### 6. Run Stage 2 — Spark Transforms
1. Open Jupyter: http://localhost:8888?token=spark
2. Run `notebooks/02-spark-transforms.ipynb`
3. Run `02b`–`02g` for additional analytics tables

### 7. Create Kafka topics (Stage 3)
```bash
docker exec -it medistream-kafka bash /kafka-init/create-topics.sh
```

### 8. Run Stage 3 — Streaming
1. Open `notebooks/03b-streaming-consumer.ipynb`, run all cells (it will block at `awaitTermination`)
2. In another tab, open `notebooks/03a-streaming-producer.ipynb`, run all cells
3. Watch the consumer for `[batch N] emitting M alert rows`
4. Run `notebooks/03c-streaming-health-check.ipynb` to verify

### 9. Run Stage 4 — Airflow Orchestration
1. Open Airflow UI: http://localhost:8080 (Default credentials: `airflow` / `airflow`)
2. Locate the two MediStream DAGs:
   - `medistream_batch_pipeline`: Handles daily Spark transformations with Data Quality Gates.
   - `medistream_streaming_health_monitor`: Runs hourly checks on Kafka and Spark Structured Streaming.
3. Unpause both DAGs to begin orchestration.

### Verify
| Service | URL |
|---|---|
| HDFS UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Spark App UI | http://localhost:4040 (while job runs) |
| Jupyter | http://localhost:8888?token=spark |
| Kafka | localhost:9092 |
| Airflow UI | http://localhost:8080

## Project Structure
```
MediStream-Telehealth/
├── data/                                ← data files (not committed to git)
├── docker/
│   └── Dockerfile.spark                 ← custom Spark image
├── hdfs-init/
│   └── create-zones.sh                  ← HDFS zone creation
├── kafka-init/
│   └── create-topics.sh                 ← Kafka topic creation
├── notebooks/
│   ├── 02-spark-transforms.ipynb        ← Stage 2 base pipeline
│   ├── 02b-no-show-breakdown.ipynb      ← no-show breakdown
│   ├── 02c-quality-by-device-os.ipynb   ← quality by device/OS
│   ├── 02d-derived-features.ipynb       ← history score + QAV
│   ├── 02e-degraded-sessions.ipynb      ← degraded sessions
│   ├── 02f-repartition-curated.ipynb    ← repartition curated tables
│   ├── 02g-followup-health-check.ipynb  ← Stage 2 health check
│   ├── 03a-streaming-producer.ipynb     ← Stage 3 producer
│   ├── 03b-streaming-consumer.ipynb     ← Stage 3 consumer
│   └── 03c-streaming-health-check.ipynb ← Stage 3 health check
├── docker-compose.yml
├── hadoop.env
└── .gitignore
```

## Windows Users
- Use WSL2 terminal for all commands
- Run `git config --global core.autocrlf false` before cloning
