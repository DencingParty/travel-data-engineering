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

# 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 자원 사용량 로깅 함수
def log_resource_usage(step):
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"[{step}] CPU: {psutil.cpu_percent()}%, Memory Usage: {mem_info.rss / (1024 ** 2):.2f} MB")
    
# 기본 경로 설정 (절대 경로)
BASE_PATH = "/opt/airflow/data"

# 데이터 필터링 함수
def filter_by_date_region(dir1="aihub", dir2="2022", start_date="2022-06-01", end_date="2022-06-06"):
    
    # 기본 경로 설정
    base_path = os.path.join(BASE_PATH, dir1, dir2, "total_combined", "region_data")

    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Base path not found: {base_path}")

    dataset_list = os.listdir(base_path)

    region_dfs = {}
    # region_gps_dfs = {}

    # gps_data, region_data 폴더 내부를 for loop으로 들어간다
    for dataset in dataset_list:
        dataset_path = os.path.join(BASE_PATH, dataset)
        # dataset_list = os.listdir(f_path)

        # for dataset in dataset_list:
        #     file_path = os.path.join(folder_path, dataset)

        if not dataset.endswith('.csv'):
            print(f"파일 스킵: {dataset} (CSV 파일 아님)")
            continue

        try:
            # 파일 읽기 시도
            df = pd.read_csv(dataset_path)
            df_name = dataset.replace(".csv", "")
            
            # 파일에 데이터가 없는 경우 스킵
            if df.empty:
                print(f"파일 스킵: {dataset} (데이터 없음)")
                continue

            # 데이터프레임에서 "YMD"로 끝나는 컬럼 찾는다 (START_DATE_YMD는 제외)
            ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]

            if not ymd_columns:
                print(f"YMD 컬럼 없음: {dataset}\n")

                # if folder == "gps_data":
                #     region_gps_dfs[df_name] = df
                # else:
                region_dfs[df_name] = df
                
                continue
            
            if len(ymd_columns) > 1:
                ymd_columns = [ymd_columns[-1]]

            print(f"데이터셋: {dataset}\nYMD 컬럼: {ymd_columns}\n")

            # YMD 컬럼 처리
            for ymd_col in ymd_columns:
                df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")

            # 날짜 필터링 (ymd_columns 중 첫 번째 컬럼)
            filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]

            # df_name = dataset.replace(".csv", "")

            # if folder == "gps_data":
            #     region_gps_dfs[df_name] = filtered_df
            # else:
            region_dfs[df_name] = filtered_df

        except pd.errors.EmptyDataError:
            print(f"파일 스킵: {dataset} (비어 있는 파일)")
        except Exception as e:
            print(f"파일 처리 오류: {dataset}: {e}")

    print(f"===================== {dir2} 데이터 필터링 완료 ({start_date} ~ {end_date}) =====================\n")

    # print(f"===================== 데이터 필터링 완료 (시작 날짜 {start_date}) =====================")
    return region_dfs

# gps 데이터 필터링 함수
def filter_by_date_gps(df, ymd_col="DT_YMD", start_date="2022-06-01", end_date="2022-06-06"):

    df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")
    filtered_df = df[(df[ymd_col] >= start_date) & (df[ymd_col] <= end_date)]

    return filtered_df

def upload_to_s3(dataframes, bucket_name, s3_prefix):
    # Airflow Connections에서 자격 증명 가져오기
    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")

    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")  # 기본 region 설정
    )
    # S3 클라이언트 생성
    s3_client = session.client('s3') # 세션에서 S3 클라이언트 생성
    failed_files = []

    for name, df in dataframes.items():
        s3_key = f"{s3_prefix}/{name}.parquet"  # 파일 확장자를 Parquet로 설정
        try:
            buffer = io.BytesIO()  # Parquet는 바이너리 포맷이므로 BytesIO 사용
            df.to_parquet(buffer, index=False, engine='pyarrow')  # Parquet 형식으로 변환
            buffer.seek(0)
            s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
            print(f"Successfully uploaded: {s3_key}")
        except Exception as e:
            print(f"Error uploading {s3_key}: {e}")
            failed_files.append(s3_key)

    if failed_files:
        raise Exception(f"Failed to upload files: {failed_files}")

# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval=None,  # 테스트용 수동 실행
    start_date=datetime(2023, 6, 1),
    catchup=False,
    description="Filter and upload data to S3 in a single task using TaskFlow API",
)
def travel_etl_test_dag():
    @task
    def filter_and_upload():
        try:
            logger.info("Starting filter_and_upload task")
            log_resource_usage("Task Start")

            # GPS 데이터 준비
            path_parquet_22 = os.path.join(BASE_PATH, "aihub", "2022", "total_combined", "gps_data_parquet", "total_gps_2022.parquet")
            logger.info(f"Loading GPS data in chunks from: {path_parquet_22}")
            
            # 청크 단위로 데이터를 읽기
            parquet_file = pq.ParquetFile(path_parquet_22)
            filtered_gps_dfs_22 = {}
            
            for i, batch in enumerate(parquet_file.iter_batches(batch_size=100000)):  # 청크 크기 설정
                logger.info(f"Processing batch {i + 1}")
                chunk_df = batch.to_pandas()
                filtered_chunk = filter_by_date_gps(chunk_df, "DT_YMD", "2022-08-10", "2022-09-09")
                filtered_gps_dfs_22[f"gps_filtered_chunk_{i + 1}"] = filtered_chunk
                log_resource_usage(f"After Processing Batch {i + 1}")

            # S3 업로드
            logger.info("Uploading GPS data to S3")
            upload_to_s3(filtered_gps_dfs_22, "travel-de-storage", "test/gps")
            log_resource_usage("After Uploading GPS Data")

            logger.info("filter_and_upload task completed successfully")

        except Exception as e:
            logger.error(f"An error occurred in filter_and_upload: {e}", exc_info=True)
            raise

    # 단일 Task 실행
    filter_and_upload()

dag = travel_etl_test_dag()