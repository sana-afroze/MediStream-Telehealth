# MediStream Telehealth Analytics Platform
## ISM 6562 — Final Project

## Team
- Stage 1: HDFS Data Lake — @sana-afroze
- Stage 2: Spark Batch Transformation — TBD
- Stage 3: Kafka Streaming — TBD
- Stage 4: Airflow Orchestration — TBD

## Quick Start

### Prerequisites
- Docker Desktop running
- Data files (get from course repo or teammate)

### 1. Clone the repo
```bash
git clone https://github.com/sana-afroze/MediStream-Telehealth.git
cd MediStream-Telehealth
```

### 2. Add data files
Place these 5 files into the `data/` folder:
appointments.csv.gz
patient-vitals.json.gz
session-quality.csv.gz
patient-feedback.json.gz
physician-schedule.csv.gz
### 3. Start HDFS cluster
```bash
docker compose up -d
```
Wait 60 seconds for all containers to be healthy.

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

### 6. Verify
- HDFS UI: http://localhost:9870
- Check Datanodes tab — should show 3 datanodes

## Project Structure
MediStream-Telehealth/
├── data/                  ← data files (not committed to git)
├── hdfs-init/             ← HDFS zone creation script
│   └── create-zones.sh
├── notebooks/             ← Jupyter notebooks (Stage 2)
├── spark/
│   └── jobs/              ← Spark Python scripts (Stage 2)
├── scripts/               ← helper scripts
├── docker-compose.yml     ← HDFS cluster setup
└── .gitignore

## Windows Users
- Use WSL2 terminal for all commands
- Run `git config --global core.autocrlf false` before cloning
