# Stage 3: Architecture & Infrastructure

## What got added to `docker-compose.yml`

Two new services in the existing `medistream-network`:

### `zookeeper` (bitnami/zookeeper:3.9)

Kafka 3.6 still uses Zookeeper for cluster metadata in the version we're running (KRaft is GA but the bitnami image used here is the ZK-mode build to match Week 10). It runs single-node — no quorum, no replication — because it's a demo cluster.

- Port: `2181`
- Memory: 512 MB cap
- Healthcheck: `echo ruok | nc localhost 2181` returns `imok`

### `kafka` (bitnami/kafka:3.6)

Single-broker Kafka. Two listeners are configured:

| Listener | Port | Used by |
|---|---|---|
| `EXTERNAL` | `9092` | Things on the host (`localhost:9092`) — useful if you want to poke from outside Docker |
| `INTERNAL` | `9093` | Other containers in `medistream-network` (`kafka:9093`) — what producer/consumer notebooks use |

The two-listener setup is necessary because containers reach the broker as `kafka:9093` (compose DNS), but if you ran a desktop tool against `localhost:9092` it would also work. Both are PLAINTEXT — fine for a class project, never production.

Other notable config:

- `auto.create.topics.enable=false` — forces topics to be created explicitly via `create-topics.sh`. Keeps partition counts under our control.
- `default.partitions=4`, `default.replication.factor=1` — single-broker means RF=1 is mandatory; 4 partitions matches `session-metrics` for parallelism.
- `log.retention.hours=24` — global default; `create-topics.sh` overrides per-topic where needed.

### Volumes

- `zookeeper_data:/bitnami/zookeeper` — persists ZK metadata across `docker compose down`
- `kafka_data:/bitnami/kafka` — persists topic data and offsets

If you `docker compose down -v`, both wipe. If you `docker compose down` (no `-v`), they survive.

### Memory budget

After Stage 3 services land, total cap is roughly:

| Service | Cap | Notes |
|---|---|---|
| namenode + 3 datanodes | 4 × 1 GB = 4 GB | Stage 1 |
| spark-master | 512 MB | Stage 2 |
| spark-worker × 2 | 2 × 2 GB = 4 GB | Stage 2 |
| jupyter | uncapped (~1 GB typical) | Stage 2 |
| zookeeper | 512 MB | Stage 3 |
| kafka | 1 GB | Stage 3 |
| **Total** | **~11 GB** | within the 16 GB README requirement |

If your laptop is tight, the safest things to lower are the spark workers (drop to 1 GB each) before touching anything else.

## Topic design (`kafka-init/create-topics.sh`)

| Topic | Partitions | Retention | Why |
|---|---|---|---|
| `session-metrics` | 4 | 24 h | Higher partition count to parallelize across both Spark workers (each runs 2 cores → 4 task slots total). 24 h is plenty for replay of a demo run. |
| `session-alerts` | 2 | 7 d | Lower volume — alerts are a small fraction of total samples. Longer retention lets the audit notebook read a week back. |

Both keyed by `session_id` so all samples for one session always land in the same partition (preserves per-session ordering, which matters for windowed aggregation).

## Why not KRaft?

KRaft (Kafka Raft) replaces Zookeeper in Kafka 3.3+ and is the future. We use ZK-mode here for two reasons:

1. The Week 10 weekly assignment uses ZK + Kafka — keeping the same shape means patterns from that work transfer directly.
2. The bitnami image in ZK mode is simpler to configure for a single-broker dev cluster (KRaft mode wants a fixed `node.id` + `controller.quorum.voters`).

For production at MediStream, KRaft would be the right call — one less service to operate, no ZK gotchas. For a class demo, ZK is the path of least friction.

## Why a separate `medistream-kafka` container name (not just `kafka`)?

Compose service name is `kafka` (other containers reach it as such), but `container_name: medistream-kafka` makes it unambiguous in `docker ps` output, matching the existing `medistream-zookeeper` and `hadoop-namenode` naming convention.
