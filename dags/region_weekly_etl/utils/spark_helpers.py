import logging
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from region_weekly_etl.config import S3_BUCKET_NAME, RAW_FOLDER, PROCESSED_FOLDER  # S3 관련 설정값 가져오기

logger = logging.getLogger(__name__)

def trigger_spark_job(start_date, end_date, credentials):
    """
    Spark 작업을 트리거하는 함수입니다. SparkSubmitOperator를 사용하여 Spark 작업을 실행합니다.
    :param start_date: 처리할 데이터의 시작 날짜
    :param end_date: 처리할 데이터의 끝 날짜
    :param credentials: AWS 자격 증명
    """
    # SparkSubmitOperator로 Spark 작업을 트리거합니다.
    spark_submit_task = SparkSubmitOperator(
        task_id="transform_region_data",
        application="/opt/airflow/spark/transform_region_data.py",  # 실행할 Spark 애플리케이션 경로
        conn_id="spark_default",  # Spark 연결 ID
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
        jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar",  # S3 연동을 위한 jar 파일
    )

    try:
        # Spark 작업을 실행합니다.
        spark_submit_task.execute(context={})
        logger.info(f"Spark 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        logger.error(f"Spark 작업 실행 중 오류가 발생했습니다: {e}")
        raise