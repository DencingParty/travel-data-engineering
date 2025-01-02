from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import boto3
import pandas as pd
import os
import io
import logging
import psutil # 메모리 및 CPU 사용량 확인을 위한 라이브러리
import pyarrow.parquet as pq

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 자원 사용량 로깅 함수
def log_resource_usage(step):
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"[{step}] CPU: {psutil.cpu_percent()}%, Memory Usage: {mem_info.rss / (1024 ** 2):.2f} MB")

# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"
S3_BUCKET_NAME = "travel-de-storage"
S3_FOLDER = "raw-data"

# S3 클라이언트 생성 함수
def get_s3_client():
    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
    )
    return session.client("s3")

# 날짜 형식 변환 함수
def transform_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%y%m%d")  # 날짜를 220102 형식으로 변환
    except ValueError:
        return date_str  # 날짜 형식이 아니면 그대로 반환

# 주 단위 필터링 설정 함수
def set_filtering_date(start_date, end_date, freq="7D"):
    return {
        f"week_{i+1}": {
            "start_date": str(start.date()),
            "end_date": str((start + pd.Timedelta(days=6)).date())
        }
        for i, start in enumerate(pd.date_range(start=start_date, end=end_date, freq=freq))
    }

# GPS 데이터 필터링 함수
def filter_by_date_gps(df, ymd_col, start_date, end_date):
    df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")
    return df[(df[ymd_col] >= start_date) & (df[ymd_col] <= end_date)]


# S3 업로드 함수 (주 단위 폴더 구조로 저장)
def upload_to_s3(s3_client, dataframes, bucket_name, base_s3_prefix, week_start):
    """
    모든 청크를 S3에 업로드한 후, YYMMDD 폴더 구조로 저장
    """
    failed_files = []

    # week_start를 YYMMDD 형식으로 변환
    folder_date = transform_date(week_start)
    folder_prefix = f"{base_s3_prefix}/{folder_date}/gps_data"  # 폴더 경로 수정

    total_chunks = len(dataframes)  # 전체 청크 수

    for i, df in enumerate(dataframes, start=1):
        file_name = f"gps_data_{folder_date}_chunk_{i}.parquet"  # 파일 이름 수정
        s3_key = f"{folder_prefix}/{file_name}"  # 최종 S3 경로 설정

        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
        except Exception as e:
            logger.error(f"Failed to upload file {file_name} to S3: {str(e)}")
            failed_files.append(s3_key)

    if not failed_files:
        logger.info(f"Successfully uploaded {total_chunks} chunks for {week_start} to S3: {bucket_name}/{folder_prefix}")
    else:
        logger.error(f"Failed to upload {len(failed_files)} files: {failed_files}")
        raise Exception(f"Failed to upload files: {failed_files}")


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
    description="Extract initial GPS data to S3",
)

def GPS_initial_extract_dag():
    @task
    def filter_and_upload():
        # 소요 시간이 너무 오래 걸려 3차례로 나눠서 진행.
        # 1. 2022-01-02 ~ 2022-06-04
        # 2. 2022-06-05 ~ 2022-12-31
        # 3. 2023-01-01 ~ 2023-06-03
        start_date = "2022-01-02"  # 테스트용 시작일
        end_date = "2023-06-03"    # 테스트용 종료일
        logger.info("Starting filter_and_upload task")
        log_resource_usage("Task Start")
        weekly_dict = set_filtering_date(start_date, end_date)

        # S3 클라이언트 생성 (한 번만)
        s3_client = get_s3_client()

        paths = {
            "2022": os.path.join(BASE_PATH, "aihub", "2022", "total_combined", "gps_data_parquet", "total_gps_2022.parquet"),
            "2023": os.path.join(BASE_PATH, "aihub", "2023", "total_combined", "gps_data_parquet", "total_gps_2023.parquet"),
        }

        for folder_date, date_range in weekly_dict.items():
            week_start = date_range["start_date"]  # 시작 날짜
            week_end = date_range["end_date"]
            logger.info(f"Processing {folder_date}: {week_start} ~ {week_end}")

            # 필터링된 청크 수집
            filtered_chunks = []

            # 2022 데이터 필터링
            if week_start.startswith("2022") or week_end.startswith("2022"):
                parquet_file = pq.ParquetFile(paths["2022"])
                for batch in parquet_file.iter_batches(batch_size=100000):
                    chunk_df = batch.to_pandas()
                    filtered = filter_by_date_gps(chunk_df, "DT_YMD", week_start, week_end)
                    if not filtered.empty:
                        filtered_chunks.append(filtered)

            # 2023 데이터 필터링
            if week_start.startswith("2023") or week_end.startswith("2023"):
                parquet_file = pq.ParquetFile(paths["2023"])
                for batch in parquet_file.iter_batches(batch_size=100000):
                    chunk_df = batch.to_pandas()
                    filtered = filter_by_date_gps(chunk_df, "DT_YMD", week_start, week_end)
                    if not filtered.empty:
                        filtered_chunks.append(filtered)

            # S3에 업로드 시 week_start를 사용
            if filtered_chunks:
                upload_to_s3(s3_client, filtered_chunks, S3_BUCKET_NAME, S3_FOLDER, week_start)
                log_resource_usage(f"After Uploading {folder_date}")
                logger.info(f"Finished uploading for date range: {week_start} ~ {week_end}")

    filter_and_upload()


dag = GPS_initial_extract_dag()
