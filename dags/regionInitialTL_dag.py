from airflow.decorators import task, dag
# from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import boto3
import os
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 기본 설정
S3_BUCKET_NAME = "travel-de-storage"
RAW_FOLDER = "raw-data"
PROCESSED_FOLDER = "processed-data"

# S3 클라이언트 생성
# def get_s3_client():
#     aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
#     session = boto3.Session(
#         aws_access_key_id=aws_conn.login,
#         aws_secret_access_key=aws_conn.password,
#         region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
#     )
#     return session.client("s3")

# DAG 정의
@dag(
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
    description="Transform and Load initial Region data to S3",
)

def region_initial_tl_dag():

    @task
    def trigger_spark_job():
        """
        SparkSubmitOperator를 통해 Spark job을 실행
        Region 데이터를 변환하고 S3에 적재
        """


        spark_submit_task = SparkSubmitOperator(
            task_id="transform_region_data",
            application="/opt/airflow/spark/transform_region_data.py",
            conn_id="spark_default",
            application_args=[
                "--input_s3_path", f"s3://{S3_BUCKET_NAME}/{RAW_FOLDER}/",
                "--output_s3_path", f"s3://{S3_BUCKET_NAME}/{PROCESSED_FOLDER}/"
            ],
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
            }
        )
        return spark_submit_task.execute(context=None)

    trigger_spark_job()

dag = region_initial_tl_dag()