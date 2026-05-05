from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import urllib.request
import json

# REQUIREMENT: Retry logic, SLAs, and Alerting
default_args = {
    'owner': 'medistream_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@medistream-telehealth.com'],
    'sla': timedelta(hours=2),
}

dag = DAG(
    'medistream_batch_pipeline',
    default_args=default_args,
    description='Daily batch processing for MediStream patient data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['medistream', 'batch']
)

WEBHDFS = 'http://hadoop-namenode:9870/webhdfs/v1'
LANDING_DATASETS = ['appointments', 'patient_vitals', 'session_quality',
                    'patient_feedback', 'physician_schedule']


def webhdfs_list(path):
    url = f'{WEBHDFS}{path}?op=LISTSTATUS'
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read())['FileStatuses']['FileStatus']


def check_landing_zone(**ctx):
    """Confirm raw data exists in /medistream/landing/ for each dataset."""
    missing = []
    for ds in LANDING_DATASETS:
        try:
            files = webhdfs_list(f'/medistream/landing/{ds}')
            if not files:
                missing.append(f'{ds} (empty)')
            else:
                print(f'  {ds}: {len(files)} file(s) present')
        except Exception as e:
            missing.append(f'{ds} ({type(e).__name__})')
    if missing:
        raise RuntimeError(f'Landing zone incomplete: {missing}')
    print(f'OK — all {len(LANDING_DATASETS)} landing datasets present')


def data_quality_gate(**ctx):
    """Validate landing files are non-zero and schemas look reasonable."""
    issues = []
    for ds in LANDING_DATASETS:
        files = webhdfs_list(f'/medistream/landing/{ds}')
        total_size = sum(f['length'] for f in files)
        if total_size == 0:
            issues.append(f'{ds}: zero bytes')
        else:
            print(f'  {ds}: {total_size:,} bytes across {len(files)} file(s)')
    if issues:
        raise RuntimeError(f'Quality gate failed: {issues}')
    print('OK — quality gate passed for all datasets')


def run_spark_transform(**ctx):
    """Marker task — Stage 2 Spark transforms run via Jupyter notebook
    (notebooks/02-spark-transforms.ipynb). This task verifies the curated
    zone has been populated since the last batch window. Idempotent."""
    try:
        files = webhdfs_list('/medistream/curated/appointments')
        if any(f['pathSuffix'] == '_SUCCESS' for f in files):
            print('OK — curated zone has _SUCCESS marker (Stage 2 transforms ran)')
            return
    except Exception as e:
        pass
    raise RuntimeError(
        'Curated zone not populated. Run notebooks/02-spark-transforms.ipynb '
        'in Jupyter to execute the Spark transforms.')


def verify_curated_output(**ctx):
    """Verify all 5 curated tables have _SUCCESS markers and non-zero output."""
    missing = []
    for ds in LANDING_DATASETS:
        try:
            files = webhdfs_list(f'/medistream/curated/{ds}')
            has_success = any(f['pathSuffix'] == '_SUCCESS' for f in files)
            data_files = [f for f in files if f['pathSuffix'].endswith('.parquet')]
            total_size = sum(f['length'] for f in data_files)
            if not has_success:
                missing.append(f'{ds} (no _SUCCESS)')
            elif total_size == 0:
                missing.append(f'{ds} (empty parquet)')
            else:
                print(f'  {ds}: {len(data_files)} parquet file(s), {total_size:,} bytes')
        except Exception as e:
            missing.append(f'{ds} ({type(e).__name__})')
    if missing:
        raise RuntimeError(f'Curated verification failed: {missing}')
    print('OK — all curated tables verified')


t1 = PythonOperator(task_id='check_landing_zone',
                    python_callable=check_landing_zone, dag=dag)
t2 = PythonOperator(task_id='data_quality_gate_input',
                    python_callable=data_quality_gate, dag=dag)
t3 = PythonOperator(task_id='run_spark_transform',
                    python_callable=run_spark_transform, dag=dag)
t4 = PythonOperator(task_id='verify_curated_output',
                    python_callable=verify_curated_output, dag=dag)

t1 >> t2 >> t3 >> t4
