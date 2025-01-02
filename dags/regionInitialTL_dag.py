from airflow.decorators import task, dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime, timedelta
import boto3
import os


# 'aws_default'는 Airflow에서 설정한 Connection ID입니다.
aws_hook = AwsBaseHook(aws_conn_id='AWS_CONNECTION_ID', client_type='s3')
credentials = aws_hook.get_credentials()

# 기본 설정
S3_BUCKET_NAME = "travel-de-storage"
RAW_FOLDER = "raw-data-test-tek"
PROCESSED_FOLDER = "processed-data-test-tek"

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
                "--input_s3_path", f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/",
                "--output_s3_path", f"s3a://{S3_BUCKET_NAME}/{PROCESSED_FOLDER}/",
                "--aws_access_key", credentials.access_key,
                "--aws_secret_key", credentials.secret_key,
                "--start_date", "2022-01-02",
                "--end_date", "2023-06-03",
            ],
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
                # S3A 파일시스템 설정 추가
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": credentials.access_key,
                "spark.hadoop.fs.s3a.secret.key": credentials.secret_key,
            },
            jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
        )
        return spark_submit_task.execute(context=None)

    trigger_spark_job()

dag = region_initial_tl_dag()