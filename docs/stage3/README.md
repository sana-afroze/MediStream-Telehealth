# Stage 3: Real-Time Streaming Pipeline — Overview

## What it does

Watches active video-consultation sessions in real time. When average call quality (latency, packet loss, audio score) crosses a threshold over a 2-minute sliding window, fires a structured alert that goes to:

1. A Kafka `session-alerts` topic — for downstream services that would push the alert to the patient/physician UI in production
2. HDFS `/medistream/analytics/streaming_alerts/` — Parquet, partitioned by `alert_date`, for dashboards and audit

## Data flow

```
session-quality.csv.gz                        (curated/landing source data)
        ↓ replayed by 03a-streaming-producer
Kafka topic: session-metrics                  (4 partitions, keyed by session_id)
        ↓ consumed by 03b-streaming-consumer
        ↓   parse JSON, withWatermark(2 min), window(2 min, slide 30 s)
        ↓   filter: avg breaches threshold
        ↓   broadcast-join with curated/appointments dim (physician, specialty)
        ↓   foreachBatch → two sinks ↓
                          ↓                              ↓
              Kafka: session-alerts            HDFS: /streaming_alerts/
              (downstream consumers)           (Parquet, partitioned by alert_date)
```

## Design choices and why

| Decision | Choice | Reason |
|---|---|---|
| Kafka mode | Zookeeper + Kafka (bitnami) | Matches Week 10 assignment patterns; familiar muscle memory |
| Window | 2 min sliding, 30 s slide | Per brief; slide is small enough to detect ~30 s degradations |
| Watermark | 2 min | Matches window length — bounds streaming state |
| Alert thresholds | latency >500 ms, packet_loss >5%, audio <5 | Same as `02c`/`02e` Stage 2 batch — batch and streaming agree on what "degraded" means |
| Enrichment | Broadcast join, not stream-stream join | Appt dim is ~550 k rows ≈ 50 MB; trivially broadcasts; avoids state explosion |
| Dual sink | Kafka + HDFS via `foreachBatch` | Kafka for live consumers, HDFS for audit/replay/dashboarding |
| Producer rate | Configurable (TODO) | Demo-friendly; user sizes for their laptop |

## How to run (in order)

1. `docker compose up -d` — wait for HDFS, Spark, Kafka to all be healthy (~2 min total)
2. `docker exec -it medistream-kafka bash /kafka-init/create-topics.sh`
3. Open Jupyter at http://localhost:8888?token=spark
4. Run **`03b-streaming-consumer.ipynb`** all the way through to the `awaitTermination()` cell. It will block, waiting for events.
5. In a *separate browser tab*, run **`03a-streaming-producer.ipynb`** all cells. It publishes events at the configured rate.
6. Watch the consumer's output stream — `[batch N] emitting M alert rows` lines should appear once windows fill.
7. After the producer finishes (or any time after a few minutes), run **`03c-streaming-health-check.ipynb`** to verify everything wired up correctly.

## File map

| File | Purpose |
|---|---|
| `docker-compose.yml` | adds `zookeeper` + `kafka` services |
| `kafka-init/create-topics.sh` | creates `session-metrics` (4 part.) and `session-alerts` (2 part.) |
| `notebooks/03a-streaming-producer.ipynb` | replays `session_quality` as a Kafka stream (1 TODO: rate) |
| `notebooks/03b-streaming-consumer.ipynb` | windowed alerts + dual sink (3 TODOs: thresholds, alert_type, suggested_action) |
| `notebooks/03c-streaming-health-check.ipynb` | end-to-end verification of topics + Parquet outputs |
| `docs/stage3/architecture.md` | infrastructure walkthrough |
| `docs/stage3/producer.md` | producer walkthrough |
| `docs/stage3/consumer.md` | consumer walkthrough |
| `docs/stage3/todos.md` | how to think about each TODO (no solutions — does not spoil it) |
