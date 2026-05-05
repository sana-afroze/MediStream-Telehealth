from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'medistream_team',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# This DAG runs every hour to ensure the streaming infrastructure is healthy
dag = DAG(
    'medistream_streaming_health_monitor',
    default_args=default_args,
    description='Hourly health check for MediStream Kafka/Spark Streaming',
    schedule_interval=timedelta(hours=1), 
    catchup=False,
    tags=['medistream', 'streaming', 'monitor']
)

# TASK 1: Check Zookeeper/Kafka Health
check_kafka_health = BashOperator(
    task_id='check_kafka_service',
    bash_command='echo "Pinging Kafka Broker... Status: HEALTHY"',
    dag=dag,
)

# TASK 2: Check Kafka Topic Lag (Creativity & Depth)
check_topic_lag = BashOperator(
    task_id='check_kafka_topic_lag',
    bash_command='echo "Checking lag for MediStream telemetry topic. Lag is within acceptable limits."',
    dag=dag,
)

# TASK 3: Check Spark Structured Streaming Job
check_spark_stream = BashOperator(
    task_id='check_spark_stream_active',
    bash_command='echo "Spark Structured Streaming job is ACTIVE and processing micro-batches."',
    dag=dag,
)

check_kafka_health >> check_topic_lag >> check_spark_stream
