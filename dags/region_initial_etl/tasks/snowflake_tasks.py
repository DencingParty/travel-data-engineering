from airflow.decorators import task
import logging

from region_initial_etl.utils.snowflake_helpers import initial_run_sql

logger = logging.getLogger(__name__)

@task
def execute_snowflake_reset_tables():
    """
    Snowflake REGION_RAW_DATA 작업
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="reset_tables_for_initial_data.sql",
        schema=schema,
    )

@task
def execute_snowflake_copy_data_from_s3():
    """
    Snowflake REGION_RAW_DATA 작업
    """
    schema = "RAW_DATA"

    logger.info(f"Snowflake {schema} 작업을 시작합니다.")
    initial_run_sql(
        initial_sql_filename="copy_into_snowflake.sql",
        schema=schema,
    )