from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import pandas as pd
import os
import logging
from io import BytesIO
from pytz import utc
from jinja2 import Template

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

aws_hook = AwsBaseHook(aws_conn_id='AWS_CONNECTION_ID', client_type='s3')
credentials = aws_hook.get_credentials()

# S3 설정
S3_BUCKET_NAME = "travel-de-storage"
RAW_FOLDER = "raw-data"
PROCESSED_FOLDER = "processed-data"

# SQL 파일 경로 설정
SQL_DIR = "/opt/airflow/sql"

# 테스트 모드 설정
TEST_MODE = True  # True면 10분 간격, False면 주간 실행

# 스케줄 간격 설정
schedule_interval = "*/15 * * * *" if TEST_MODE else "@weekly"

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

#  S3 버킷에서 지정된 prefix 하위 폴더 중 특정 날짜 이후 데이터를 삭제하는 함수 (metadata 폴더는 제외)
def clean_s3_folder(bucket_name, prefix, start_date="2023-06-04"):
    s3_client = get_s3_client() # S3 클라이언트 가져오기
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    # prefix 하위 객체 목록 가져오기
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # 'Contents' 키가 없을 경우 빈 리스트를 반환하므로 별도 조건문 없이 반복문 실행 가능
    for obj in response.get('Contents', []):     # 'Contents' 키가 없으면 빈 리스트를 반환
        key = obj['Key']
        folder_date_str = key.split('/')[1]

        # 메타데이터라면 바로 continue
        if folder_date_str == "metadata":
            logger.info(f"메타데이터 폴더는 건너뜁니다: {key}.")
            continue

        try:
            # 폴더 이름을 날짜로 변환
            folder_date = datetime.strptime(folder_date_str, "%y%m%d")

            # start_date 이후 데이터인지 확인
            if folder_date >= start_date:
                # S3 객체 삭제
                s3_client.delete_object(Bucket=bucket_name, Key=key)
                logger.info(f"S3에서 {key}를 삭제했습니다.")

        except ValueError:
            # ValueError 예외 처리와 메시지 간결화
            logger.warning(f"날짜 형식이 아닌 폴더를 건너뜁니다: {folder_date_str}")

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
                    filtered_df = df
                    region_dfs[df_name] = filtered_df
                    continue
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
                if "tc_" in file_name or "tn_activity_his_" in file_name or "tn_traveller_master_" in file_name:
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

# Snowflake SQL 파일 실행 함수
def run_sql_task(sql_filename, task_type, sql_params):
    """
    SQL 파일을 읽어 SnowflakeHook를 통해 실행합니다.
    """
    
    sql_path = os.path.join(SQL_DIR, sql_filename)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} 파일을 찾을 수 없습니다.")
    
    with open(sql_path, "r") as sql_file:
        sql_template = sql_file.read()
    
    # Jinja2 템플릿 렌더링
    try:
        if sql_params:
            template = Template(sql_template)
            sql_query = template.render(sql_params)
        else:
            sql_query = sql_template
    except Exception as e:
        logger.error(f"SQL 템플릿 렌더링 중 오류 발생 ({task_type}): {str(e)}")
        raise e

    # Snowflake 연결 및 SQL 실행
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    try:
        # Snowflake 연결
        logger.info(f"SQL 쿼리 파일 실행 중 ({task_type}): {sql_filename}")
        snowflake_conn = snowflake_hook.get_conn()
        cursor = snowflake_conn.cursor()

        # SQL 문을 세미콜론으로 분리하여 개별적으로 실행
        statements = [query.strip() for query in sql_query.split(';') if query.strip()]
        for idx, statement in enumerate(statements, start=1):
            logger.info(f"실행 중인 SQL 쿼리 {idx}/{len(statements)}: {statement[:200]}...")
            cursor.execute(statement)
            logger.info(f"Statement {idx} affected {cursor.rowcount} rows.")

        logger.info(f"모든 SQL 문을 성공적으로 실행했습니다. ({task_type}): {sql_filename}")

    except Exception as e:
        # 실행 중 에러 발생 시 로그 기록 및 재전파
        logger.error(f"SQL 파일 실행 중 오류 발생 ({task_type}): {sql_filename}: {str(e)}")
        raise e

    finally:
        # 커넥션 종료
        cursor.close()
        snowflake_conn.close()

def sql_once_or_scheduled(run_state, sql_filename_once=None, sql_filename_scheduled=None, schema="RAW_DATA", start_date=None):
    """
    SQL 파일을 읽어 SnowflakeHook를 통해 실행합니다.
    run_state에 따라 초기 작업 -> schedule 작업 순으로 실행하거나, schedule 작업만 실행합니다.
    """
    
    # 동적으로 사용할 날짜 변수
    sql_params = {"start_date": start_date} 

    if not sql_filename_scheduled:
        raise ValueError("schedule 작업을 위해 sql_filename_scheduled이 필요합니다.")

    logger.info(f"작업 중인 날짜: {start_date}")
    logger.info(f"작업 중인 Snowflake Schema: {schema}")

    # 초기 작업 (공통 처리)
    if run_state in ["pending", "force_reset"]:
        if not sql_filename_once:
           logger.info(f"{schema} sql_filename_once가 없습니다. 초기 작업을 건너뜁니다.")
        else:
            reset_mode = "reset" if schema == "RAW_DATA" else "running-once"
            logger.info(f"run_state가 {run_state}입니다. Snowflake {schema} 스키마의 초기 작업을 수행합니다. ({reset_mode})")
            run_sql_task(sql_filename_once, reset_mode, sql_params)

    else:
        logger.info(f"run_state가 {run_state} 입니다. Snowflake {schema} 스키마의 초기 작업을 건너뜁니다.")

    # schedule 작업 (공통 처리)
    schedule_mode = "scheduled"
    logger.info(f"Snowflake {schema} 스키마의 데이터를 스케쥴링하여 처리합니다. ({schedule_mode})")
    run_sql_task(sql_filename_scheduled, schedule_mode, sql_params)

def get_task_context():
    """
    DAG 실행을 위한 컨텍스트를 반환합니다.
    Airflow에서 컨텍스트: 태스크 실행과 관련된 정보의 집합 (DAG 실행 정보, 환경 변수, 사용자 정의 데이터 등)
    """

    run_state = Variable.get(RUN_STATE_FLAG, default_var="pending")
    forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
    start_date = (START_DATE + timedelta(weeks=((forced_date - START_DATE).days // 7) - 1)).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    next_date = (datetime.strptime(CURRENT_DATE, "%Y-%m-%d") + timedelta(weeks=1)).strftime("%Y-%m-%d")

    return {
        "run_state": run_state,
        "forced_date": forced_date,
        "start_date": start_date,
        "end_date": end_date,
        "next_date": next_date
    }

# DAG 정의
@dag(
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    schedule_interval=schedule_interval,  # 동적 스케줄링
    start_date=START_DATE,  # 스케쥴링 첫 실행 날짜
    catchup=False,
    description="Weekly ETL to filter region data and upload to S3",
)

def region_weekly_etl_dag():

    context = get_task_context()

    @task
    def reset_variables_and_clean_s3(**kwargs):
        """
        최초 DAG 실행 또는 강제 초기화 시 S3 데이터 클리닝 및 Variable 초기화 수행.
        """
        run_state = context["run_state"]
        dag_run = kwargs.get("dag_run")
        logger.info("초기화 작업을 시작합니다.")
        # run_state = Variable.get(RUN_STATE_FLAG, default_var="pending")

        if dag_run:
            logger.info(f"DAG run_type: {dag_run.run_type}, RUN_STATE_FLAG: {run_state}")

        # 수동 트리거 시 run_state가 'force_reset'이 아니면 초기화 건너뛰기
        if dag_run and dag_run.run_type == "manual" and run_state != "force_reset":
            logger.info("DAG가 수동으로 trigger 되었습니다. S3 초기화 작업을 건너뜁니다.")
            return

        # 최초 실행(pending) 또는 강제 초기화(force_reset)인 경우 초기화 수행
        if run_state in ["pending", "force_reset"]:
            logger.info("Variable 및 S3 데이터 초기화를 수행합니다.")

            # Variable 초기화
            Variable.set("current_date", "2023-06-11")
            logger.info("Variable 초기화가 완료되었습니다.")

            # S3 데이터 클리닝
            logger.info("S3 데이터 클리닝을 시작합니다.")
            clean_s3_folder(S3_BUCKET_NAME, RAW_FOLDER, start_date="2023-06-04")
            clean_s3_folder(S3_BUCKET_NAME, PROCESSED_FOLDER, start_date="2023-06-04")
            logger.info("S3 데이터 정리가 완료되었습니다.")
        
        else:
            logger.info("초기화 작업을 건너뜁니다. DAG가 이전에 이미 실행되었습니다.")

    @task
    def process_region_data(execution_date=None):

        start_date = context["start_date"]
        end_date = context["end_date"]

        logger.info(f"데이터 처리 기간: {start_date} ~ {end_date}")

        # 데이터 필터링
        region_data = filter_by_date_region("aihub", "2023", start_date, end_date)

        # 필터링된 데이터 S3 업로드
        if region_data:
            upload_to_s3(region_data, start_date, is_gps=False)  # is_gps를 True/False로 설정

    # Transform & Load: Spark Job을 실행하여 Region 데이터를 변환하고 S3에 적재
    @task
    def trigger_spark_job():

        logger.info("Spark 작업을 시작합니다.")    

        start_date = context["start_date"]
        end_date = context["end_date"]

        spark_submit_task = SparkSubmitOperator(
            task_id="transform_region_data",
            application="/opt/airflow/spark/transform_region_data.py",
            conn_id="spark_default",
            application_args=[
                "--input_s3_path", f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/",
                "--output_s3_path", f"s3a://{S3_BUCKET_NAME}/{PROCESSED_FOLDER}/",
                "--aws_access_key", credentials.access_key,
                "--aws_secret_key", credentials.secret_key,
                "--start_date", start_date,
                "--end_date", end_date,
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

        spark_result = spark_submit_task.execute(context={})
        logger.info(f"Spark 작업이 성공적으로 완료되었습니다.")

        return spark_result

    @task
    def execute_snowflake_sql(schema, sql_filename_once, sql_filename_scheduled):
        """
        공통 SQL 실행 로직을 처리하는 동적 Task 생성 함수.
        """
        run_state = context["run_state"]
        start_date = datetime.strptime(context["start_date"], "%Y-%m-%d").strftime("%y%m%d")

        logger.info(f"Snowflake {schema} 작업을 시작합니다")
        sql_once_or_scheduled(
            run_state,
            sql_filename_once=sql_filename_once,
            sql_filename_scheduled=sql_filename_scheduled,
            schema=schema,
            start_date=start_date,
        )
        logger.info(f"Snowflake {schema} 작업이 완료되었습니다.")

    # Variable 자동 업데이트 태스크
    @task
    def update_variable():
        next_date = context["next_date"]
        
        Variable.set(RUN_STATE_FLAG, "done")
        Variable.set("current_date", next_date)
        logger.info(f"Variable 'current_date'가 {next_date}(으)로 업데이트 되었습니다.")

    # Snowflake Task 설정 리스트
    snowflake_task_configs = [
        {"schema": "raw_data", "sql_filename_once": "reset_to_initial_state_region.sql", "sql_filename_scheduled": "schedule_append_region.sql"},
        {"schema": "ad_hoc", "sql_filename_once": "once_for_ad_hoc.sql", "sql_filename_scheduled": "schedule_for_ad_hoc.sql"},
        {"schema": "processed_data", "sql_filename_once": "once_for_processed_data.sql", "sql_filename_scheduled": "schedule_for_processed_data.sql"},
        {"schema": "analysis", "sql_filename_once": None, "sql_filename_scheduled": "schedule_for_analysis.sql"},
    ]

    # 동적으로 Task 생성 및 순차 연결
    previous_task = reset_variables_and_clean_s3() >> process_region_data() >> trigger_spark_job()

    # 동적으로 Snowflake Task 생성하고 순차적으로 연결
    # 명시적으로 Task 이름을 설정하기 위해 @task 데코레이터에서 override() 설정
    for config in snowflake_task_configs:
        current_task = execute_snowflake_sql.override(task_id=f"execute_snowflake_sql_{config['schema']}")(
            schema=config["schema"],
            sql_filename_once=config["sql_filename_once"],
            sql_filename_scheduled=config["sql_filename_scheduled"],
        )
        previous_task >> current_task  # 이전 Task와 연결
        previous_task = current_task  # 현재 Task를 다음 Task의 부모로 설정
    
    # 마지막 Task
    previous_task >> update_variable()

    # DAG 실행 순서
    # reset_variables_and_clean_s3() >> process_region_data() >> trigger_spark_job() >> execute_raw_data_sql_snowflake() >> execute_ad_hoc_sql_snowflake() >> execute_processed_data_sql_snowflake() >> execute_analysis_sql_snowflake() >> update_variable()

dag = region_weekly_etl_dag()