# MediStream Telehealth Analytics Platform
## ISM 6562 — Final Project

## Team
- Stage 1: HDFS Data Lake — @sana-afroze
- Stage 2: Spark Batch Transformation — @svela
- Stage 3: Kafka Streaming — TBD
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

### 3. Start the full stack (HDFS + Spark)
```bash
docker compose up -d
```
Wait ~60 seconds for HDFS, then ~30 more seconds for Spark.

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

### 7. Verify
| Service | URL |
|---|---|
| HDFS UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Spark App UI | http://localhost:4040 (while job runs) |
| Jupyter | http://localhost:8888?token=spark |

## Project Structure
```
MediStream-Telehealth/
├── data/                       ← data files (not committed to git)
├── docker/
│   └── Dockerfile.spark        ← custom Spark image (uid-aligned)
├── hdfs-init/
│   └── create-zones.sh         ← HDFS zone creation script
├── notebooks/
│   └── 02-spark-transforms.ipynb  ← Stage 2 PySpark notebook
├── docker-compose.yml          ← HDFS + Spark cluster setup
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
         ↕
┌─────────────────────────────────────────────┐
│  Spark Cluster (Stage 2)                    │
│  Master → Worker 1 (2 cores, 2 GB)         │
│         → Worker 2 (2 cores, 2 GB)         │
│  Jupyter (PySpark Driver)                   │
└─────────────────────────────────────────────┘
```

## Windows Users
- Use WSL2 terminal for all commands
- Run `git config --global core.autocrlf false` before cloning
