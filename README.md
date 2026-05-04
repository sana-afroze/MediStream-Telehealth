# MediStream Telehealth Analytics Platform
## ISM 6562 — Final Project

## Team
- Stage 1: HDFS Data Lake — @sana-afroze
- Stage 2: Spark Batch Transformation — @svela
- Stage 3: Kafka Streaming — @Harshxth
- Stage 4: Airflow Orchestration — TBD

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
Download from: `https://github.com/prof-tcsmith/6562S26-data/tree/main/final-projects/04-medistream-telehealth/`

### 3. Start the full stack (HDFS + Spark + Kafka)
```bash
docker compose up -d
```
Wait ~60 seconds for HDFS, ~30 more for Spark, ~20 more for Kafka.

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
2. Navigate to `notebooks/02-spark-transforms.ipynb`
3. Run all cells in order

### 7. Create Kafka topics (Stage 3)
```bash
docker exec -it medistream-kafka bash /kafka-init/create-topics.sh
```
Creates `session-metrics` (4 partitions) and `session-alerts` (2 partitions).

### 8. Run Stage 3 — Streaming
1. Open `notebooks/03b-streaming-consumer.ipynb` in Jupyter, run all cells. The `awaitTermination()` cell blocks waiting for events.
2. In a separate browser tab, open `notebooks/03a-streaming-producer.ipynb` and run all cells. It will publish events to Kafka.
3. Watch the consumer notebook — `[batch N] emitting M alert rows` lines appear once windows fill.
4. After a few minutes, run `notebooks/03c-streaming-health-check.ipynb` to verify.

Detailed walkthroughs live in [`docs/stage3/`](docs/stage3/).

### 9. Verify
| Service | URL |
|---|---|
| HDFS UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Spark App UI | http://localhost:4040 (while job runs) |
| Jupyter | http://localhost:8888?token=spark |
| Kafka broker | localhost:9092 (external), kafka:9093 (internal) |

## Project Structure
```
MediStream-Telehealth/
├── data/                                ← data files (not committed to git)
├── docker/
│   └── Dockerfile.spark                 ← custom Spark image (uid-aligned)
├── hdfs-init/
│   └── create-zones.sh                  ← HDFS zone creation script
├── kafka-init/
│   └── create-topics.sh                 ← Kafka topic creation script (Stage 3)
├── notebooks/
│   ├── 02-spark-transforms.ipynb        ← Stage 2 base pipeline
│   ├── 03a-streaming-producer.ipynb     ← Stage 3 producer (replays session-quality)
│   ├── 03b-streaming-consumer.ipynb     ← Stage 3 windowed alert consumer
│   └── 03c-streaming-health-check.ipynb ← Stage 3 end-to-end verification
├── docs/
│   └── stage3/                          ← Stage 3 design + walkthrough docs
│       ├── README.md
│       ├── architecture.md
│       ├── producer.md
│       ├── consumer.md
│       └── todos.md
├── docker-compose.yml                   ← HDFS + Spark + Kafka cluster setup
├── hadoop.env
└── .gitignore
```

## Architecture
```
┌─────────────────────────────────────────────┐
│  HDFS Data Lake (Stage 1)                   │
│  ┌─────────┐  ┌─────────┐  ┌───────────┐   │
│  │ Landing │→ │ Curated │→ │ Analytics │   │
│  │ (raw)   │  │ (clean) │  │ (agg)     │   │
│  └─────────┘  └─────────┘  └───────────┘   │
│       ↑              ↕              ↕        │
│  Load data     PySpark reads/writes          │
└─────────────────────────────────────────────┘
         ↕                       ↕
┌─────────────────────────────────────────────┐
│  Spark Cluster (Stage 2 + Stage 3)         │
│  Master → Worker 1 (2 cores, 2 GB)         │
│         → Worker 2 (2 cores, 2 GB)         │
│  Jupyter (PySpark Driver)                   │
└─────────────────────────────────────────────┘
                        ↕
┌─────────────────────────────────────────────┐
│  Kafka (Stage 3)                            │
│  Zookeeper → Kafka broker                   │
│  Topics:                                    │
│    session-metrics  (raw quality samples)   │
│    session-alerts   (windowed degradation)  │
│  Spark Structured Streaming consumes        │
│  session-metrics, emits alerts to both      │
│  session-alerts and HDFS analytics zone.    │
└─────────────────────────────────────────────┘
```

## Windows Users
- Use WSL2 terminal for all commands
- Run `git config --global core.autocrlf false` before cloning
