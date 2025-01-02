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
from io import BytesIO

# 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 자원 사용량 로깅 함수
# def log_resource_usage(step):
#     process = psutil.Process()
#     mem_info = process.memory_info()
#     logger.info(f"[{step}] CPU: {psutil.cpu_percent()}%, Memory Usage: {mem_info.rss / (1024 ** 2):.2f} MB")
    
# 기본 경로 설정 (절대 경로)
BASE_PATH = "/opt/airflow/data"

# 데이터 필터링 함수
def filter_by_date_region(dir1="aihub", dir2="2022", start_date="2022-06-01", end_date="2022-06-06"):
    
    # 기본 경로 설정 (data\aihub\2022)
    if start_date.startswith("2022"):       
        base_path = os.path.join(BASE_PATH, dir1, dir2, "total_combined", "region_data")
    else:
        dir2 = "2023"
        base_path = os.path.join(BASE_PATH, dir1, dir2, "total_combined", "region_data")

    dataset_list = os.listdir(base_path)

    region_dfs = {}

    # gps_data, region_data 폴더 내부를 for loop으로 들어간다
    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)

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

# Region 데이터 초기 값 세팅
def get_weekly_region(weekly_dict):
    region_dict = {}
    
    for key in weekly_dict:

        start_date = weekly_dict[key]["start_date"]
        end_date = weekly_dict[key]["end_date"]

        region_dict[start_date] = filter_by_date_region(dir1="aihub", dir2="2022", 
                                                        start_date=start_date, end_date=end_date)
    
    return region_dict

# GPS 데이터 초기 값 세팅
def get_weekly_gps(weekly_dict, path_parquet, year):
    logger.info(f"Processing GPS data for {year} from file: {path_parquet}")

    # Parquet 파일 청크 단위로 읽기
    parquet_file = pq.ParquetFile(path_parquet)
    chunk_size = 100_000  # 청크 크기 설정

    # 각 청크를 읽어 반복 처리
    for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
        logger.info(f"Processing batch {i + 1} for year {year}")

        # 청크를 데이터프레임으로 변환
        chunk_df = batch.to_pandas()

        # 주별 데이터 필터링
        for week, date_range in weekly_dict.items():
            start_date = date_range["start_date"]
            end_date = date_range["end_date"]

            # logger.info(f"Filtering GPS data for {week}: {start_date} ~ {end_date}")
            filtered_chunk = filter_by_date_gps(chunk_df, "DT_YMD", start_date, end_date)

            # 필터링된 데이터가 있을 경우 반환
            if not filtered_chunk.empty:
                yield week, year, i + 1, filtered_chunk

# 날짜 형식 변환 함수
def transform_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%y%m%d")  # 날짜를 220102 형식으로 변환
    except ValueError:
        return date_str  # 날짜 형식이 아니면 그대로 반환

def upload_to_s3(data, is_gps, start_date, chunk_num):
    # Airflow Connections에서 자격 증명 가져오기
    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")

    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")  # 기본 region 설정
    )
    # S3 클라이언트 생성
    s3_client = session.client('s3')  # 세션에서 S3 클라이언트 생성

    # S3 버킷 이름과 폴더 설정
    bucket_name = 'travel-de-storage'
    s3_folder = 'raw-data'

    # 현재 경로 업데이트
    transformed_key = transform_date(start_date)
    if is_gps:
        base_path = f"{transformed_key}/gps_data"
        file_name = f"gps_data_{transformed_key}_chunk_{chunk_num}"
    else:
        base_path = f"{transformed_key}/region_data"
        file_name = f"region_data_{transformed_key}_chunk_{chunk_num}"

    s3_key = f"{s3_folder}/py_test/{base_path}/{file_name}.parquet"

    # DataFrame을 Parquet로 변환
    parquet_buffer = BytesIO()
    data.to_parquet(parquet_buffer, index=False)

    # S3 업로드
    try:
        print(f"Uploading DataFrame to {s3_key}...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )
        print(f"Successfully uploaded: {s3_key}\n")
    except Exception as e:
        print(f"Error uploading {s3_key}: {e}\n")



# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 2022-01-02 ~ 2023-06-03 필터링 기간 설정 (초기 값)
weekly_default_dict = {
    f"week_{i+1}": {
        "start_date": str(start.date()),
        "end_date": str((start + pd.Timedelta(days=6)).date())
    }
    for i, start in enumerate(pd.date_range(start="2022-01-02", end="2022-07-09", freq="7D"))
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
    def process_region_data():
        try:
            logger.info("Starting process_region_data task")

            # Region 데이터 필터링
            weekly_region_data = get_weekly_region(weekly_default_dict)

            # S3 업로드
            logger.info("Uploading Region data to S3")
            upload_to_s3(weekly_region_data, is_gps=False)
            logger.info("Region data successfully uploaded to S3")

        except Exception as e:
            logger.error(f"Error in process_region_data: {e}", exc_info=True)
            raise

    # 2. 주별 GPS 데이터 처리 및 업로드
    @task
    def process_gps_data():
        try:
            logger.info("Starting process_gps_data task")

            # Parquet 파일 경로 정의
            path_parquet_22 = os.path.join(BASE_PATH, "aihub", "2022", "total_combined", 
                                        "gps_data_parquet", "total_gps_2022.parquet")
            path_parquet_23 = os.path.join(BASE_PATH, "aihub", "2023", "total_combined", 
                                        "gps_data_parquet", "total_gps_2023.parquet")

            # 2022년 데이터 처리
            for week, year, batch_num, filtered_chunk in get_weekly_gps(weekly_default_dict, path_parquet_22, "2022"):
                start_date = weekly_default_dict[week]["start_date"]  # 주의 시작 날짜
                logger.info(f"Uploading GPS data for {year}, {week}, batch {batch_num}")
                upload_to_s3(filtered_chunk, is_gps=True, start_date=start_date, chunk_num=batch_num)

            # 2023년 데이터 처리
            # for week, year, batch_num, filtered_chunk in get_weekly_gps(weekly_default_dict, path_parquet_23, "2023"):
            #     start_date = weekly_default_dict[week]["start_date"]  # 주의 시작 날짜
            #     logger.info(f"Uploading GPS data for {year}, {week}, batch {batch_num}")
            #     upload_to_s3(filtered_chunk, is_gps=True, start_date=start_date, chunk_num=batch_num)

            logger.info("GPS data processing and upload completed successfully")

        except Exception as e:
            logger.error(f"Error in process_gps_data: {e}", exc_info=True)
            raise

    # 실행 순서 정의
    process_gps_data() >> process_region_data()

dag = travel_etl_test_dag()