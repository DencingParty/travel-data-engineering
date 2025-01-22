from airflow.decorators import task
from airflow.models import Variable
import logging

from region_weekly_etl.utils.variable_helpers import get_task_context
from region_weekly_etl.utils.s3_helpers import clean_s3_folder, upload_to_s3
from region_weekly_etl.utils.data_filtering_helpers import filter_by_date_region
from region_weekly_etl.config import S3_BUCKET_NAME, RAW_FOLDER, PROCESSED_FOLDER

logger = logging.getLogger(__name__)

# get_task_context()에서 가져오는 변수를 통해 날짜 정보 가져오기
context = get_task_context()

@task
def reset_variables_and_clean_s3(**kwargs):
    """
    최초 DAG 실행 또는 강제 초기화 시 S3 데이터 클리닝 및 Variable 초기화 수행.
    """
    run_state = context["run_state"]
    dag_run = kwargs.get("dag_run")
    logger.info("초기화 작업을 시작합니다.")

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