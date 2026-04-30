# MediStream Telehealth Analytics Platform

## Quick Start (all teammates)

### Prerequisites
- Docker Desktop installed and running
  - macOS: Download from https://www.docker.com/products/docker-desktop
  - Windows: Download from https://www.docker.com/products/docker-desktop
    - ⚠️ Windows users: Enable WSL2 backend in Docker Desktop settings
    - ⚠️ Windows users: Run all commands in **WSL2 terminal** (Ubuntu), not PowerShell

### 1. Clone the repo
```bash
git clone https://github.com/Harshxth/MediStream-Telehealth.git
cd MediStream-Telehealth
```

### 2. Add data files
Download the data files from the course repo and place them in the `data/` folder: data/appointments.csv.gz
data/patient_vitals.json.gz
data/session_quality.csv.gz
data/patient_feedback.json.gz
data/physician_schedule.csv.gz
### 3. Start the environment
```bash
docker compose up -d
```

### 4. Create HDFS zones
```bash
docker exec -it namenode bash /hdfs-init/create-zones.sh
```

### 5. Verify
- HDFS UI:  http://localhost:9870
- Spark UI: http://localhost:8080
- Jupyter:  http://localhost:8888

## Project Structure
MediStream-Telehealth/
├── data/              ← data files (not committed to git)
├── hdfs-init/         ← HDFS zone setup scripts
├── spark/jobs/        ← Spark transformation scripts (Stage 2)
├── notebooks/         ← Jupyter notebooks
└── scripts/           ← helper scripts 
## Stages
- Stage 1: Data Lake Foundation (HDFS) — @sana-afroze
- Stage 2: Batch Transformation (Spark) — TBD
- Stage 3: Real-Time Streaming (Kafka) — TBD
- Stage 4: Pipeline Orchestration (Airflow) — TBD

## Windows-Specific Notes
- Always use WSL2 terminal for all commands
- If `docker compose` gives an error, try `docker-compose` (with hyphen)
- Line ending issues: run `git config --global core.autocrlf false` before cloning
