# Stage 3: Producer Walkthrough (`03a-streaming-producer.ipynb`)

## What it does, in one paragraph

Reads the `session_quality` dataset from HDFS (or local mount), pulls each row to the driver, and publishes it to Kafka topic `session-metrics` one event at a time at a configurable rate. Each event is keyed by `session_id` (so all samples for the same session route to the same partition) and value-encoded as JSON. The producer adds an `event_time` field set to wallclock-at-publish — that's what the consumer uses for windowing, not the original timestamp from the file.

## Why a producer at all

The brief frames `session_quality` as the natural streaming source — quality samples stream in real time during active appointments. We don't have a live MediStream instance to tap, so we synthesize the stream by replaying the historical file. Functionally identical from the consumer's perspective: it just sees JSON events arrive on a topic.

## Why `kafka-python` instead of Spark for publishing

Spark *can* write a static DataFrame to Kafka in one shot, but that ships everything in milliseconds — there's no "stream" for the consumer to window over. We want to space events out in time, so we use `kafka-python` and a `time.sleep(1/rate)` loop. Simple, easy to reason about, easy to throttle.

(In production, the producer would be the MediStream session service publishing in-flight metrics natively. Same Kafka topic shape, no replay needed.)

## Read order through the notebook

1. **`pip install kafka-python`** — the base `pyspark-notebook` image doesn't have it. We pin `==2.0.2` because newer 3.x has API changes that bite occasionally.
2. **Spark session** — small (512 MB driver, 512 MB executor) because the producer barely uses Spark; it's mostly there to read the source file.
3. **`load_sessions()`** — tries 4 paths in order: HDFS curated → local curated → HDFS landing → local landing. Lets the notebook run in any environment without code changes. First success wins.
4. **TODO: rate + max events** — see `todos.md`.
5. **`producer = KafkaProducer(...)`** — connects to `kafka:9093` (the internal listener; this notebook runs inside the compose network as the Jupyter container). Key serializer turns `session_id` into UTF-8 bytes. Value serializer is `json.dumps` → bytes.
6. **The publish loop** — for each row, build the JSON envelope, call `producer.send(...)`, sleep. Every 500 events it prints progress. At the end, `producer.flush()` blocks until the broker has acked everything in the local buffer.

## Per-event payload

```json
{
  "session_id":          "SESS-...",
  "appointment_id":      "APPT-...",
  "video_resolution":    "720p",
  "audio_quality_score": 7.5,
  "latency_ms":          340.0,
  "packet_loss_pct":     2.1,
  "duration_sec":        420.0,
  "device_type":         "phone",
  "os":                  "iOS",
  "event_time":          "2026-04-30T14:32:11.234567+00:00"
}
```

## Tunables to be aware of

- **`linger_ms=20`** in the producer config — the broker batches sends in 20 ms windows. Higher = better throughput, slightly higher per-message latency. 20 ms is fine for our rates.
- **`acks=1`** — wait for the leader to ack before the `.send` future resolves. `acks=0` is fire-and-forget (faster, can lose messages on broker crash). `acks='all'` requires all replicas (we only have one, so equivalent to `acks=1` here).

## Common gotchas

- **Consumer must be running first.** The consumer uses `startingOffsets='earliest'` so it'll catch up if started late, but you'll see "no events" lines for a while.
- **Notebook hangs at end of `producer.send` loop?** That's `flush()` waiting for the broker to ack. Give it 30 s; if it still hangs, check `docker logs medistream-kafka`.
- **`KafkaError: NoBrokersAvailable`?** The broker isn't healthy yet. `docker compose ps` should show `kafka (healthy)` — wait until then.

## Where to make this fancier

If you wanted to go beyond the rubric:
- **Variable rate**: bursts of high traffic between calmer periods — closer to real telehealth load.
- **Inject deliberate degradation**: every Nth event, multiply `latency_ms` by 3 to guarantee the consumer fires alerts on demand. Useful for grading-day demos where you want a visible alert within 2 minutes.
- **Multi-process producer**: `multiprocessing.Pool` to fan out across cores. Diminishing returns past ~500 ev/s on a laptop.

None of these are required.
