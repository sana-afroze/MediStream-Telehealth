#!/bin/bash
# Stage 3 — Create Kafka topics for the MediStream streaming pipeline.
#
# Run from the host:
#   docker exec -it medistream-kafka bash /kafka-init/create-topics.sh
#
# Topics:
#   session-metrics   keyed by session_id  raw quality samples from the producer
#   session-alerts    keyed by session_id  degradation events from the streaming consumer
set -e

KAFKA_BIN=/opt/bitnami/kafka/bin
BROKER=localhost:9093

echo "Creating MediStream Kafka topics on ${BROKER}..."

# session-metrics: high-throughput input topic. 4 partitions for parallelism on the
# 2-worker Spark cluster. 24h retention is plenty for a demo replay.
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic session-metrics \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=86400000

# session-alerts: lower-volume output topic. 2 partitions is enough; longer
# retention (7 days) so downstream auditors / dashboards can replay alerts.
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic session-alerts \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=604800000

echo
echo "Done. Current topics:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER --list

echo
echo "session-metrics config:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER --describe --topic session-metrics

echo
echo "session-alerts config:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BROKER --describe --topic session-alerts
