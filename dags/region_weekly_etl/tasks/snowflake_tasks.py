from airflow.decorators import task
from datetime import datetime
import logging

from region_weekly_etl.utils.snowflake_helpers import sql_once_or_scheduled
from region_weekly_etl.utils.variable_helpers import get_task_context

logger = logging.getLogger(__name__)

# get_task_context()에서 가져오는 변수를 통해 날짜 정보 가져오기
context = get_task_context()


@task
def execute_snowflake_sql_raw_data():
    """
    Snowflake REGION_RAW_DATA 작업
    """
    run_state = context["run_state"]
    start_date = datetime.strptime(context["start_date"], "%Y-%m-%d").strftime("%y%m%d")
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    sql_once_or_scheduled(
        run_state,
        sql_filename_once="reset_to_initial_state_region.sql",
        sql_filename_scheduled="schedule_append_region.sql",
        schema=schema,
        start_date=start_date,
    )

@task
def execute_snowflake_sql_processed_data():
    """
    Snowflake REGION_RAW_DATA 작업
    """
    run_state = context["run_state"]
    start_date = datetime.strptime(context["start_date"], "%Y-%m-%d").strftime("%y%m%d")
    schema = "PROCESSED_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    sql_once_or_scheduled(
        run_state,
        sql_filename_once="once_for_processed_data.sql",
        sql_filename_scheduled="schedule_for_processed_data.sql",
        schema=schema,
        start_date=start_date,
    )

@task
def execute_snowflake_sql_analysis():
    """
    Snowflake REGION_RAW_DATA 작업
    """
    run_state = context["run_state"]
    start_date = datetime.strptime(context["start_date"], "%Y-%m-%d").strftime("%y%m%d")
    schema = "ANALYSIS"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    sql_once_or_scheduled(
        run_state,
        sql_filename_once=None,
        sql_filename_scheduled="schedule_for_analysis.sql",
        schema=schema,
        start_date=start_date,
    )