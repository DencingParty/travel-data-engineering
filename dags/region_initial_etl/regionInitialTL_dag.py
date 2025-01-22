from airflow.decorators import dag
from datetime import datetime, timedelta

from region_initial_etl.tasks import trigger_spark_task, execute_snowflake_reset_tables, execute_snowflake_copy_data_from_s3

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
    description="Transform and Load initial Region data to S3",
)

def region_initial_tl_dag():
    # Spark Job을 실행하여 Region 데이터를 변환하고 S3에 적재
    trigger_spark_task() >> execute_snowflake_reset_tables() >> execute_snowflake_copy_data_from_s3()

dag = region_initial_tl_dag()