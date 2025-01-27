from airflow.decorators import dag
from datetime import datetime, timedelta
import logging

from region_initial_etl.tasks import process_region_data

# 로깅 설정
logger = logging.getLogger(__name__)

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
    
    process_region_data()

dag = region_initial_extract_dag()