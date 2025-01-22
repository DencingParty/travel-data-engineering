from airflow.decorators import task
import logging

from region_initial_etl.utils.spark_helpers import trigger_spark_job
from region_initial_etl.utils.s3_helpers import get_s3_client

logger = logging.getLogger(__name__)


@task
def trigger_spark_task():
    """
    Transform & Load: Spark Job을 실행하여 Region 데이터를 변환하고 S3에 적재
    """
    logger.info("Spark 작업을 시작합니다.")    

    # AWS 자격 증명 가져오기
    s3_client = get_s3_client()  # get_s3_client()로 S3 클라이언트 가져오기
    credentials = s3_client._request_signer._credentials  # 자격 증명 가져오기

    # Spark 작업 트리거
    trigger_spark_job(credentials)