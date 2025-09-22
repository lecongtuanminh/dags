from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_buckets():
    hook = S3Hook(aws_conn_id="minio_conn")
    # boto3 client
    client = hook.get_conn()
    response = client.list_buckets()
    for b in response.get("Buckets", []):
        print(f"Bucket: {b['Name']}")

with DAG(
    "test_minio_connection",
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:
    PythonOperator(
        task_id="list_minio_buckets",
        python_callable=check_buckets,
    )
