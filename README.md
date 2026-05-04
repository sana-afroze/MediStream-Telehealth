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
├── data/                              ← data files (not committed to git)
├── docker/
│   └── Dockerfile.spark               ← custom Spark image (uid-aligned)
├── hdfs-init/
│   └── create-zones.sh                ← HDFS zone creation script
├── notebooks/
│   ├── 02-spark-transforms.ipynb      ← Stage 2 base pipeline
│   ├── 02b-no-show-breakdown.ipynb    ← Stage 2 follow-up: Q1 no-show breakdown
│   ├── 02c-quality-by-device-os.ipynb ← Stage 2 follow-up: Q7 platform reliability
│   ├── 02d-derived-features.ipynb     ← Stage 2 follow-up: history score + QAV
│   ├── 02e-degraded-sessions.ipynb    ← Stage 2 follow-up: degraded session flags
│   ├── 02f-repartition-curated.ipynb  ← Stage 2 follow-up: partition curated tables
│   ├── 02g-followup-health-check.ipynb ← post-flight check: every output exists with expected schema
│   └── 03-stage3-readiness.ipynb      ← Stage 3 integration guide (read-only — Stage 3 is implemented separately)
├── docker-compose.yml                 ← HDFS + Spark cluster setup
├── hadoop.env
└── .gitignore
```

## Stage 2 Follow-ups

The base `02-spark-transforms.ipynb` covers the joins and high-level aggregations. Five additional notebooks (prefixed `02b`–`02f`) close gaps against the final-project brief and the rubric. **Run order:** base notebook → `02f` (one-time repartition) → `02b`–`02e` (analytics tables) in any order.

| Notebook | Output table | Partition | Brief reference |
|---|---|---|---|
| `02b-no-show-breakdown` | `analytics/no_show_breakdown` | `specialty` | Q1 — no-show prediction by specialty × time-of-day × day-of-week × visit_type |
| `02c-quality-by-device-os` | `analytics/quality_by_device_os` | `device_type` | Q7 — platform reliability, device + OS recommendations |
| `02d-derived-features` | `analytics/patient_history_scores`, `analytics/physician_quality_adjusted_volume` | `engagement_tier`, none | Stage 2 derived features: history score + rating-weighted volume |
| `02e-degraded-sessions` | `analytics/degraded_sessions` | `degraded_severity` | Stage 2 derived feature: batch view of session degradation (Stage 3 will compute the streaming view) |
| `02f-repartition-curated` | rewrites `curated/{appointments,session_quality,patient_vitals}` | per Stage 1 partitioning hints | Stage 1 partitioning hints from the brief |

**Quality thresholds** are kept consistent across batch (`02c`, `02e`) and the upcoming Stage 3 streaming alerts: `latency_ms > 500`, `packet_loss_pct > 5`, `audio_quality_score < 5`.

After running the follow-ups, run `02g-followup-health-check.ipynb` to verify every expected curated and analytics table exists with the right schema. `03-stage3-readiness.ipynb` documents what data Stage 3 will consume from Stages 1 + 2 (dimension tables, baseline lookups, alert envelope schema) — it is not the Stage 3 implementation.

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
