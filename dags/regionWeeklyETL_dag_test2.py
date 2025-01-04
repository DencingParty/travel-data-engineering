from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime, timedelta
import boto3
import pandas as pd
import os
import logging
from io import BytesIO

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

aws_hook = AwsBaseHook(aws_conn_id='AWS_CONNECTION_ID', client_type='s3')
credentials = aws_hook.get_credentials()

# S3 설정
S3_BUCKET_NAME = "travel-de-storage"
RAW_FOLDER = "raw-data-test-weekly2"
PROCESSED_FOLDER = "processed-data-test-weekly2"

# S3 클라이언트 생성 함수
def get_s3_client():
    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
    )
    return session.client("s3")

# Region 데이터 필터링 함수
def filter_by_date_region(dir1="aihub", dir2="2023", start_date="2023-06-04", end_date="2023-06-10"):
    base_path = os.path.join("/opt/airflow/data", dir1, dir2, "total_combined", "region_data_parquet")
    dataset_list = os.listdir(base_path)
    region_dfs = {}

    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)
        if not dataset.endswith('.parquet'):
            continue

        try:
            df = pd.read_parquet(dataset_path)
            df_name = dataset.replace(".parquet", "")

            if df.empty:
                continue

            ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]
            min_columns = [col for col in df.columns if col.endswith('MIN')]
            
            if not ymd_columns:
                region_dfs[df_name] = df
                continue
            
            for ymd_col in ymd_columns:
                df[ymd_col] = pd.to_datetime(df[ymd_col], format="%Y-%m-%d", errors="coerce") \
                    .dt.floor("ms") \
                    .astype("datetime64[ms]")  # ns -> ms 변환 : ns 단위를 spark가 읽지를 못해서 변경

            for min_col in min_columns:
                df[min_col] = pd.to_datetime(df[min_col], format="%H:%M:%S", errors="coerce") \
                     .dt.floor("ms") \
                     .astype("datetime64[ms]")
            
            filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]
            region_dfs[df_name] = filtered_df

        except Exception as e:
            logger.error(f"Error processing file {dataset}: {e}")

    return region_dfs

# S3 업로드 함수
def upload_to_s3(data, start_date, is_gps=False):
    s3_client = get_s3_client()
    transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    base_path = f"{transformed_key}/gps_data" if is_gps else f"{transformed_key}/region_data"
    try:
        logger.info(f"Starting S3 upload for week starting {start_date} ({base_path})")
        for file_name, df in data.items():
            if is_gps:
                file_name_key = f"gps_data_{transformed_key}"
                s3_key = f"{RAW_FOLDER}/{base_path}/{file_name_key}.parquet"
            else:
                if "tc_" in file_name or "tn_activity_his_" in file_name:
                    file_name_key = f"{file_name}"
                    s3_key = f"{RAW_FOLDER}/metadata/{file_name_key}.parquet"
                else:
                    file_name_key = f"{file_name}_{transformed_key}"
                    s3_key = f"{RAW_FOLDER}/{base_path}/{file_name_key}.parquet"
            # DataFrame을 Parquet로 변환
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            # S3 업로드
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
        logger.info(f"Successfully uploaded all files for week starting {start_date}")
    except Exception as e:
        logger.error(f"Error during S3 upload for week starting {start_date}: {e}")

# DAG 정의
@dag(
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@weekly", # 매주 실행
    start_date=datetime(2023, 6, 4), # 스케쥴링 첫 실행 날짜
    end_date=datetime(2023, 7, 2), # 스케쥴링 종료 날짜
    catchup=True,
    description="Weekly ETL to filter region data and upload to S3",
)
def region_weekly_etl_dag2():
    @task
    def region_weekly_extract_dag(data_interval_start=None):
        # 실행 주기 기반으로 데이터 범위 설정
        # start_date = execution_date.strftime("%Y-%m-%d")
        start_date = data_interval_start.strftime("%Y-%m-%d")
        # end_date = (execution_date + timedelta(days=6)).strftime("%Y-%m-%d")
        end_date = (data_interval_start + timedelta(days=6)).strftime("%Y-%m-%d")
        # 데이터 필터링
        region_data = filter_by_date_region("aihub", "2023", start_date, end_date)
        # 필터링된 데이터 S3 업로드
        if region_data:
            upload_to_s3(region_data, start_date, is_gps=False) # is_gps를 True/False로 설정

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
                "--start_date", "2023-06-04",
                "--end_date", "2023-07-02",
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
        return spark_submit_task.execute(context={})

    region_weekly_extract_dag() >> trigger_spark_job()

dag = region_weekly_etl_dag2()
