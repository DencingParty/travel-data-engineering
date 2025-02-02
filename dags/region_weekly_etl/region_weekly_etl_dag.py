from airflow.decorators import dag
import logging
from datetime import timedelta

from region_weekly_etl.config import TEST_MODE, START_DATE
from region_weekly_etl.tasks.s3_tasks import reset_variables_and_clean_s3, process_region_data
from region_weekly_etl.tasks.spark_tasks import trigger_spark_task
from region_weekly_etl.tasks.snowflake_tasks import execute_snowflake_sql_raw_data, execute_snowflake_sql_processed_data, execute_snowflake_sql_analysis
from region_weekly_etl.tasks.update_variables import update_variables

logger = logging.getLogger(__name__)

# DAG 정의
@dag(
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    schedule_interval="*/15 * * * *" if TEST_MODE else "@weekly",  # 동적 스케줄링
    start_date=START_DATE,  # 스케쥴링 첫 실행 날짜
    catchup=False,
    description="Weekly ETL to filter region data and upload to S3",
)

def region_weekly_etl_dag():
    
    # DAG 실행 순서
    reset_variables_and_clean_s3() \
    >> process_region_data() \
    >> trigger_spark_task() \
    >> execute_snowflake_sql_raw_data() \
    >> execute_snowflake_sql_processed_data() \
    >> execute_snowflake_sql_analysis() \
    >> update_variables()

dag = region_weekly_etl_dag()