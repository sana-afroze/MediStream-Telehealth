# Stage 3: Consumer Walkthrough (`03b-streaming-consumer.ipynb`)

This is the meaty notebook. It implements the actual streaming alert logic.

## What it does, in one paragraph

Reads `session-metrics` from Kafka, parses each JSON event, groups events into 2-minute sliding windows per `session_id`, computes window-average metrics, filters to windows where averages cross degradation thresholds, classifies the alert type, broadcast-joins appointment context (physician + specialty), maps a suggested user-facing action, and writes the resulting alerts to both Kafka (`session-alerts`) and HDFS (`/streaming_alerts/`, partitioned by `alert_date`).

## Cell-by-cell

### Setup (cells 1–2)

The Spark session needs `spark-sql-kafka-0-10_2.12:3.5.0` to talk to Kafka. We supply it via `spark.jars.packages`, which makes Spark download it from Maven Central on first run. Subsequent runs are cached locally so they're fast.

Checkpoint location is set to `/home/jovyan/data/checkpoints/streaming-consumer` — Spark Structured Streaming requires this so it can resume from where it left off if the query restarts.

### Input schema + `readStream` (cell 3)

`metrics_schema` is the explicit `StructType` matching the producer's payload. Type-explicit schemas beat `inferSchema` for streams because the inference would happen per-batch and could vary.

The `readStream`:
- `format('kafka')` + `subscribe(TOPIC_IN)` — the Kafka connector
- `startingOffsets='earliest'` — replay from the very beginning of the topic. For a real production deployment you'd use `'latest'`; here we want every event the producer publishes to count.
- `failOnDataLoss='false'` — won't crash if a topic gets compacted between batches. Belt-and-suspenders for a demo.

The chain after `.load()` parses the binary Kafka value as a UTF-8 string, applies `from_json` with the schema, and converts the ISO string `event_time` to a real timestamp.

### Windowed aggregation (cell 4)

```python
.withWatermark('event_time', '2 minutes')
.groupBy(F.window('event_time', '2 minutes', '30 seconds'),
         'session_id', 'appointment_id', 'device_type', 'os')
.agg(F.count(...), F.avg(...), F.avg(...), F.avg(...))
```

Two important Spark Structured Streaming concepts here:

**Window**: `F.window(col, '2 minutes', '30 seconds')` creates overlapping sliding windows. A given event time `t` belongs to multiple windows — every 30-second slide that contains `t`. This means each session's events emit ~4 alert candidates per 2-minute span (since 2min/30sec = 4). That's good for low-latency alerting; first window to cross threshold fires the alert.

**Watermark**: `withWatermark('event_time', '2 minutes')` tells Spark "I won't see events more than 2 minutes late." Spark uses this to know when a window is closed and its state can be dropped. Without a watermark, the streaming state would grow forever.

We group by additional columns (`device_type`, `os`, `appointment_id`) so they survive the aggregation — those are needed for downstream enrichment and the alert payload.

### Threshold tuning + degraded filter (cells 5–6)

**TODO #1 — thresholds**: see `todos.md`. The `assert` lines fail loudly if you forget to set them.

The `degraded` DataFrame keeps only window rows where at least one threshold is breached. `OR` not `AND` — any one signal is enough to fire.

### `alert_type` (cell 6 cont.)

**TODO #2 — alert_type classifier**: see `todos.md`. The TODO walks through three options (single most-severe / combined string / array). The tests check that you set `alert_type` to *something*.

### Enrichment via broadcast join (cell 7)

```python
appt_dim = spark.read.parquet(...).select(...)
enriched = labeled.join(F.broadcast(appt_dim), on='appointment_id', how='left')
```

`F.broadcast()` is a hint that tells Spark "ship this DataFrame in full to every executor — don't shuffle." It's the right call when one side of a join is small enough to fit in driver memory and you want to avoid the network cost of a shuffle join. For our 550k-row appointments dim, broadcast is dramatically faster than the alternative.

`how='left'` because some streamed sessions might not have an appointment row in the curated table (data lag, etc.). Better to emit the alert with `physician_id IS NULL` than drop it.

### `suggested_action` (cell 8)

**TODO #3 — suggested_action mapping**: see `todos.md`. Same shape as TODO #2 — a `F.when(...).when(...).otherwise(...)` chain returning a string column.

The notebook adds an `alert_date` column (= `to_date(window_end)`) here too, so the HDFS sink can partition by it without recomputing.

### Sink via `foreachBatch` (cells 9–10)

`foreachBatch` is the escape hatch in Spark Structured Streaming for sinks that don't have a built-in writer. Each micro-batch is handed to your callback as a regular `DataFrame`, which you can `.write` to anything you like. We write to two places:

1. **HDFS append** — `mode('append').partitionBy('alert_date').parquet(...)` — accumulates alerts into date-partitioned Parquet. Idempotent across reruns because Spark Structured Streaming's checkpoint guarantees each micro-batch is delivered exactly once when the sink is idempotent (and Parquet append is).
2. **Kafka write** — pack every column into a JSON envelope (`F.to_json(F.struct(...))`), set the key to `session_id`, write to `session-alerts`. This is the live consumer feed.

`trigger(processingTime='30 seconds')` controls how often the streaming engine wakes up to process accumulated events. 30 s is a balance — short enough to feel real-time, long enough that micro-batches have meaningful work to do.

### `awaitTermination` (cell 11)

Blocks the notebook cell until the query stops. Use the commented `query.stop()` in cell 12 to stop cleanly when you're done.

## Why these choices

| Choice | Why |
|---|---|
| Sliding window vs. tumbling | Sliding (with overlap) gives sub-window-length alert latency; tumbling would force you to wait the full 2 min for any alert |
| Watermark = window length | Standard rule of thumb; balances state size vs. tolerance for late data |
| `foreachBatch` for dual sink | Built-in Kafka sink can't combine with another sink in a single query |
| Broadcast vs. stream-stream join | Broadcast is exact, predictable, no state. Stream-stream would need its own watermark and a join-state TTL |
| `startingOffsets='earliest'` | Demo: we want everything. Prod: would be `'latest'` plus offset committing |

## Common gotchas

- **`KafkaSourceNotFoundException` on first run**: Spark is downloading the Kafka package. First run on a fresh container takes ~30 s. Re-run the cell.
- **Watermark eating events**: if your producer's `event_time` field is wallclock and the consumer is *also* doing a slow batch (e.g. you re-ran), watermark may close windows before late events arrive. Stop and restart the consumer to reset.
- **`AnalysisException: cannot call methods on an UnresolvedException`**: usually means you forgot to set one of the TODO variables. The `assert` lines should catch this earlier — if they don't, look at the TODO cells.
- **Empty alerts forever**: if your producer rate is 1 event/sec, you might publish only ~120 events into a window — could be all the same `session_id`, never breaching threshold. Bump producer rate or lower thresholds.
