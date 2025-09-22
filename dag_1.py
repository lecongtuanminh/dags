from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def list_buckets():
    hook = S3Hook(aws_conn_id="minio_conn")
    print(hook.list_buckets())

with DAG("test_s3", start_date=datetime(2023, 1, 1), schedule="@once", catchup=False) as dag:
    PythonOperator(task_id="list_buckets", python_callable=list_buckets)

