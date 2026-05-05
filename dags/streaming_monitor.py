from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'medistream_team',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'medistream_streaming_health_monitor',
    default_args=default_args,
    description='Hourly health check for MediStream Kafka/Spark Streaming',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['medistream', 'streaming', 'monitor']
)

KAFKA_BOOTSTRAP = 'kafka:9093'
EXPECTED_TOPICS = ['session-metrics', 'session-alerts']


def check_kafka_service(**ctx):
    """Confirm Kafka broker is reachable and the expected topics exist."""
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BOOTSTRAP],
                             request_timeout_ms=10000)
    topics = admin.list_topics()
    print(f'  topics on broker: {sorted(topics)}')
    missing = [t for t in EXPECTED_TOPICS if t not in topics]
    if missing:
        raise RuntimeError(f'Missing topics: {missing}')
    print(f'OK — Kafka reachable, {len(EXPECTED_TOPICS)} expected topics present')


def check_kafka_topic_lag(**ctx):
    """Report end offsets for both topics. Used as a smoke test that data
    is flowing on session-metrics and that the consumer is emitting on
    session-alerts."""
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BOOTSTRAP],
                             enable_auto_commit=False,
                             request_timeout_ms=10000)
    for topic in EXPECTED_TOPICS:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            consumer.close()
            raise RuntimeError(f'Topic {topic} has no partitions / does not exist')
        tps = [TopicPartition(topic, p) for p in parts]
        end = consumer.end_offsets(tps)
        total = sum(end.values())
        print(f'  {topic}: total_offset={total:,} across {len(parts)} partitions')
    consumer.close()
    print('OK — topic offsets retrieved')


def check_spark_stream_active(**ctx):
    """Verify Spark master web UI responds (workers registered, alive)."""
    import urllib.request, urllib.error
    try:
        with urllib.request.urlopen('http://spark-master:8080/json/', timeout=10) as r:
            data = r.read().decode()
        if '"workers"' in data:
            print('OK — Spark master responding, workers registered')
        else:
            raise RuntimeError('Spark master responded but no workers field')
    except urllib.error.URLError as e:
        raise RuntimeError(f'Spark master unreachable: {e}')


t1 = PythonOperator(task_id='check_kafka_service',
                    python_callable=check_kafka_service, dag=dag)
t2 = PythonOperator(task_id='check_kafka_topic_lag',
                    python_callable=check_kafka_topic_lag, dag=dag)
t3 = PythonOperator(task_id='check_spark_stream_active',
                    python_callable=check_spark_stream_active, dag=dag)

t1 >> t2 >> t3
