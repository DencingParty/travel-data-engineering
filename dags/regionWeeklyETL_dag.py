from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import pandas as pd
import os
import logging
from io import BytesIO
from pytz import utc

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# S3 설정
S3_BUCKET_NAME = "travel-de-storage"
S3_FOLDER = "raw-data-jh_test"

# 테스트 모드 설정
TEST_MODE = True  # True면 10분 간격, False면 주간 실행

# 스케줄 간격 설정
schedule_interval = "*/10 * * * *" if TEST_MODE else "@weekly"

# 초기 데이터 시작 시점
START_DATE = datetime(2023, 6, 4)

# 강제로 현재 시간을 2023년 6월 11일로 설정 (Variable 사용)
CURRENT_DATE = Variable.get("current_date", default_var="2023-06-11")

RUN_STATE_FLAG = "dag_run_state"  # 통합 Variable

# S3 클라이언트 생성 함수
def get_s3_client():
    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
    )
    return session.client("s3")

# 초기 S3 데이터 삭제 함수 (230604 이후 폴더 삭제)
def clean_s3_folder(bucket_name, prefix, start_date="2023-06-04"):
    s3_client = get_s3_client()
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    # S3에서 prefix 경로 하위 객체 목록 가져오기
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in objects:
        for obj in objects['Contents']:
            key = obj['Key']

            # 'metadata' 폴더 무시
            folder_date_str = key.split('/')[1]
            if folder_date_str == "metadata":
                logger.info(f"Skipping metadata folder: {key}")
                continue  # metadata 폴더는 스킵

            try:
                # 날짜 변환 시도
                folder_date = datetime.strptime(folder_date_str, "%y%m%d")
                
                # 지정된 start_date 이후 데이터 삭제
                if folder_date >= start_date:
                    s3_client.delete_object(Bucket=bucket_name, Key=key)
                    logger.info(f"Deleted {key} from S3")
            except ValueError:
                logger.warning(f"Skipping non-date folder: {folder_date_str}")
    else:
        logger.info("No files found for cleanup.")

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

            if not ymd_columns:
                region_dfs[df_name] = df
                continue

            for ymd_col in ymd_columns:
                df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")

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
    schedule_interval=schedule_interval,  # 동적 스케줄링
    start_date=START_DATE,  # 스케쥴링 첫 실행 날짜
    catchup=False,
    description="Weekly ETL to filter region data and upload to S3",
)
def region_weekly_extract_dag():
    @task
    def reset_variables_and_clean_s3(**kwargs):
        """
        최초 DAG 실행 또는 강제 초기화 시 S3 데이터 클리닝 및 Variable 초기화 수행.
        """
        dag_run = kwargs.get("dag_run")
        run_state = Variable.get(RUN_STATE_FLAG, default_var="pending")

        # 수동 트리거 시 run_state가 'force_reset'이 아니면 초기화 건너뛰기
        if dag_run and dag_run.run_type == "manual" and run_state != "force_reset":
            logger.info("DAG manually triggered. Skipping S3 clean-up.")
            return

        # 최초 실행(pending) 또는 강제 초기화(force_reset)인 경우 초기화 수행
        if run_state in ["pending", "force_reset"]:
            logger.info("Performing S3 clean-up and Variable reset...")

            # Variable 초기화
            Variable.set(RUN_STATE_FLAG, "done")
            Variable.set("current_date", "2023-06-11")
            logger.info("Variables have been reset.")

            # S3 데이터 클리닝
            logger.info("Starting S3 data clean-up...")
            clean_s3_folder(S3_BUCKET_NAME, S3_FOLDER, start_date="2023-06-04")
            logger.info("S3 clean-up completed.")
        else:
            logger.info("S3 clean-up skipped. DAG has already run before.")

    @task
    def process_region_data(execution_date=None):
        forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
        start_date = (START_DATE + timedelta(weeks=((forced_date - START_DATE).days // 7) - 1)).strftime("%Y-%m-%d")
        end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

        logger.info(f"Processing data for period: {start_date} to {end_date}")

        # 데이터 필터링
        region_data = filter_by_date_region("aihub", "2023", start_date, end_date)

        # 필터링된 데이터 S3 업로드
        if region_data:
            upload_to_s3(region_data, start_date, is_gps=False)  # is_gps를 True/False로 설정

    # Variable 자동 업데이트 태스크
    @task
    def update_variable():
        next_date = (datetime.strptime(CURRENT_DATE, "%Y-%m-%d") + timedelta(weeks=1)).strftime("%Y-%m-%d")
        Variable.set("current_date", next_date)
        logger.info(f"Updated current_date to {next_date}")

    # DAG 실행 순서
    reset_variables_and_clean_s3() >> process_region_data() >> update_variable()

dag = region_weekly_extract_dag()





# from airflow.decorators import task, dag
# from airflow.hooks.base import BaseHook
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
# from datetime import datetime, timedelta
# import boto3
# import pandas as pd
# import pyarrow.parquet as pq
# from io import BytesIO
# import os
# import logging



# # 로깅 설정
# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger(__name__)


# # 기본 경로 설정
# BASE_PATH = "/opt/airflow/data"

# # S3 설정
# S3_BUCKET_NAME = "travel-de-storage"
# S3_FOLDER = "raw-data-test-weekly"

# # S3 클라이언트 생성 함수
# def get_s3_client():
#     aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
#     session = boto3.Session(
#         aws_access_key_id=aws_conn.login,
#         aws_secret_access_key=aws_conn.password,
#         region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
#     )
#     return session.client("s3")

# def set_filtering_date(start_date="2023-06-04", end_date="2023-12-31", freq="7D"):
#     weekly_dict = {
#         f"week_{i+1}": {
#             "start_date": str(start.date()),
#             "end_date": str((start + pd.Timedelta(days=6)).date())
#         }
#         for i, start in enumerate(pd.date_range(start=start_date, end=end_date, freq=freq))
#     }

#     return weekly_dict

# # Region 데이터 필터링 함수
# def filter_by_date_region(dir1="aihub", dir2="2022", start_date="2022-01-02", end_date="2022-01-08"):
#     base_path = os.path.join("/opt/airflow/data", dir1, dir2, "total_combined", "region_data_parquet")
#     dataset_list = os.listdir(base_path)
#     region_dfs = {}

#     for dataset in dataset_list:
#         dataset_path = os.path.join(base_path, dataset)
#         if not dataset.endswith('.parquet'):
#             continue

#         try:
#             df = pd.read_parquet(dataset_path)
#             df_name = dataset.replace(".parquet", "")
#             if df.empty:
#                 continue

#             ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]
#             min_columns = [col for col in df.columns if col.endswith('MIN')]

#             if not ymd_columns:
#                 region_dfs[df_name] = df
#                 continue

#             if len(ymd_columns) > 1:
#                 ymd_columns = [ymd_columns[-1]]

#             for ymd_col in ymd_columns:
#                 df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce").dt.floor("ms").astype("datetime64[ms]") # ns -> ms 변환 : ns 단위를 spark가 읽지를 못해서 변경

#             for min_col in min_columns:
#                 df[min_col] = pd.to_datetime(df[min_col], errors="coerce").dt.floor("ms").astype("datetime64[ms]")

#             filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]
#             region_dfs[df_name] = filtered_df

#         except Exception as e:
#             logger.error(f"Error processing file {dataset}: {e}")

#     return region_dfs

# # S3 업로드 함수
# def upload_to_s3(data, start_date, is_gps=False):
#     s3_client = get_s3_client()
#     transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
#     base_path = f"{transformed_key}/gps_data" if is_gps else f"{transformed_key}/region_data"
    
#     try:
#         logger.info(f"Starting S3 upload for week starting {start_date} ({base_path})")

#         for file_name, df in data.items():
#             if is_gps:
#                 file_name_key = f"gps_data_{transformed_key}"
#                 s3_key = f"{S3_FOLDER}/{base_path}/{file_name_key}.parquet"
#             else:
#                 if "tc_" in file_name or "tn_activity_his_" in file_name:
#                     file_name_key = f"{file_name}"
#                     s3_key = f"{S3_FOLDER}/metadata/{file_name_key}.parquet"
#                 else:
#                     file_name_key = f"{file_name}_{transformed_key}"
#                     s3_key = f"{S3_FOLDER}/{base_path}/{file_name_key}.parquet"

#             # DataFrame을 Parquet로 변환
#             parquet_buffer = BytesIO()
#             df.to_parquet(parquet_buffer, engine='pyarrow', index=False, coerce_timestamps='ms')

#             # S3 업로드
#             s3_client.put_object(
#                 Bucket=S3_BUCKET_NAME,
#                 Key=s3_key,
#                 Body=parquet_buffer.getvalue()
#             )

#         logger.info(f"Successfully uploaded all files for week starting {start_date}")

#     except Exception as e:
#         logger.error(f"Error during S3 upload for week starting {start_date}: {e}")

# # DAG 정의
# @dag(
#     default_args={
#         "owner": "airflow",
#         "depends_on_past": False,
#         "retries": 1,
#         "retry_delay": timedelta(minutes=5),
#     },
#     schedule_interval='@weekly',
#     start_date=datetime(2023, 6, 4),
#     catchup=True,
#     description="Extract Weekly Region Data to S3"
# )

# def region_weekly_extract_dag():
#     @task
#     def process_region_data():
#         weekly_dict = set_filtering_date(start_date="2023-06-04", end_date="2023-12-31", freq="7D")
#         for week, date_range in weekly_dict.items():
#             start_date = date_range["start_date"]
#             end_date = date_range["end_date"]

#             # Filter data for the week
#             region_data = filter_by_date_region("aihub", "2023", start_date, end_date)

#             # Upload filtered data to S3
#             if region_data:
#                 upload_to_s3(region_data, start_date)

#     process_region_data()

# dag = region_weekly_extract_dag()