from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import os
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# 기본 경로 설정
BASE_PATH = "/opt/airflow/data"

# S3 설정
S3_BUCKET_NAME = "travel-de-storage"
S3_FOLDER = "raw-data-test-tek"

# S3 클라이언트 생성 함수
def get_s3_client():
    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
    )
    return session.client("s3")

def set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D"):
    weekly_dict = {
        f"week_{i+1}": {
            "start_date": str(start.date()),
            "end_date": str((start + pd.Timedelta(days=6)).date())
        }
        for i, start in enumerate(pd.date_range(start=start_date, end=end_date, freq=freq))
    }

    return weekly_dict

# Region 데이터 필터링 함수
def filter_by_date_region(dir1="aihub", dir2="2022", start_date="2022-01-02", end_date="2022-01-08"):
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

            if len(ymd_columns) > 1:
                ymd_columns = [ymd_columns[-1]]

            for ymd_col in ymd_columns:
                df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce").dt.floor("ms").astype("datetime64[ms]") # ns -> ms 변환 : ns 단위를 spark가 읽지를 못해서 변경

            for min_col in min_columns:
                df[min_col] = pd.to_datetime(df[min_col], errors="coerce").dt.floor("ms").astype("datetime64[ms]")

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
                s3_key = f"{S3_FOLDER}/{base_path}/{file_name_key}.parquet"
            else:
                if "tc_" in file_name or "tn_activity_his_" in file_name:
                    file_name_key = f"{file_name}"
                    s3_key = f"{S3_FOLDER}/metadata/{file_name_key}.parquet"
                else:
                    file_name_key = f"{file_name}_{transformed_key}"
                    s3_key = f"{S3_FOLDER}/{base_path}/{file_name_key}.parquet"

            # DataFrame을 Parquet로 변환
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False, coerce_timestamps='ms')

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
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
    description="Extract initial region data to S3",
)
def region_initial_extract_dag():
    @task
    def process_region_data():
        weekly_dict = set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D")
        for week, date_range in weekly_dict.items():
            start_date = date_range["start_date"]
            end_date = date_range["end_date"]

            # Filter data for the week
            region_data = filter_by_date_region("aihub", "2022", start_date, end_date)

            # Upload filtered data to S3
            if region_data:
                upload_to_s3(region_data, start_date)

    process_region_data()

dag = region_initial_extract_dag()