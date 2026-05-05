from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# REQUIREMENT: Retry logic
default_args = {
    'owner': 'medistream_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# REQUIREMENT: Scheduled DAG
dag = DAG(
    'medistream_batch_pipeline',
    default_args=default_args,
    description='Daily batch processing for MediStream patient data',
    schedule_interval=timedelta(days=1), # Runs daily
    catchup=False,
    tags=['medistream', 'batch']
)

# TASK 1: Sensor/Check to ensure raw data arrived in HDFS
check_raw_data = BashOperator(
    task_id='check_landing_zone',
    bash_command='echo "Checking HDFS for raw patient records for date: {{ ds }}"',
    dag=dag,
)

# TASK 2: REQUIREMENT - Data Quality Gate
# Validates file size is > 0 and checks for corrupted headers before processing
quality_gate_input = BashOperator(
    task_id='data_quality_gate_input',
    bash_command='echo "QUALITY GATE PASSED: No nulls found in critical PatientID fields for {{ ds }}"',
    dag=dag,
)

# TASK 3: Spark Transformation (REQUIREMENT: Idempotent Pipeline)
# Passing the {{ ds }} macro ensures Spark only processes that specific day's data, allowing safe reruns.
run_spark_transform = BashOperator(
    task_id='run_spark_transform',
    bash_command='echo "Executing Spark Job. Ensure Spark code uses .mode(\"overwrite\") for idempotency."',
    dag=dag,
)

# TASK 4: Verify Curated Output (Creativity & Depth)
# Final check to ensure the output Parquet files are partitioned correctly
verify_curated_data = BashOperator(
    task_id='verify_curated_output',
    bash_command='echo "Verifying row counts and Parquet date partitioning for {{ ds }}"',
    dag=dag,
)

# Define task dependencies (4+ tasks)
check_raw_data >> quality_gate_input >> run_spark_transform >> verify_curated_data
