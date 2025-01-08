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
RAW_FOLDER = "raw-data-st-initial-test"

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
def filter_by_date_region(dir1="aihub", dir2="2023", start_date="2023-06-04", end_date="2023-06-10"):
    base_path = os.path.join("/opt/airflow/data", dir1, dir2, "total_combined", "region_data_parquet")
    
    try:
        dataset_list = [f for f in os.listdir(base_path) if f.endswith('.parquet')]
    except FileNotFoundError:
        logger.error(f"디렉토리를 찾을 수 없습니다: {base_path}")
        return {}

    region_dfs = {}
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)

        try:
            df = pd.read_parquet(dataset_path)
            if df.empty:
                logger.warning(f"데이터프레임이 비어 있습니다: {dataset}")
                continue

            df_name = dataset.replace(".parquet", "")
            
            # 날짜 및 시간 컬럼 필터링
            ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]
            min_columns = [col for col in df.columns if col.endswith('MIN')]

            # 날짜 및 시간 변환
            for col in ymd_columns + min_columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").astype('datetime64[ms]')  # 밀리초로 변환
                if col in ymd_columns:  # 날짜만 유지
                    df[col] = df[col].dt.date  # datetime.date 객체로 변환

            # 날짜 필터링
            if ymd_columns:
                if df_name.startswith("tn_traveller_master"):
                    ymd_columns[0] = "TRAVEL_STATUS_END_YMD"
                logger.info(f"{df_name}의 필터링 기준 컬럼: {ymd_columns[0]}")
                filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]
            else:
                logger.info(f"{df_name}에는 YMD 컬럼이 없습니다.")
                filtered_df = df

            region_dfs[df_name] = filtered_df

        except Exception as e:
            logger.error(f"{dataset} 파일 처리 중 오류가 발생했습니다: {e}")
            continue

    return region_dfs

# S3에 데이터를 업로드
def upload_to_s3(data, start_date, is_gps=False):
    s3_client = get_s3_client()
    
    # 시작 날짜를 변환하여 경로의 일부로 사용 (YYMMDD 형식)
    transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    
    # GPS 여부에 따라 base_path를 설정
    base_path = f"{transformed_key}/gps_data" if is_gps else f"{transformed_key}/region_data"

    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

    try:
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 업로드합니다 ({base_path})")

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
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False, coerce_timestamps="ms") 


            # 변환된 Parquet을 S3 업로드
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )

        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드가 완료되었습니다.")

    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}")

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
        # weekly_dict = set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D")
        weekly_dict = set_filtering_date(start_date="2022-08-07", end_date="2022-08-14", freq="7D")
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