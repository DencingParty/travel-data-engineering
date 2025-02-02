import logging
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from region_initial_etl.config import S3_BUCKET_NAME, RAW_FOLDER, PROCESSED_FOLDER  # S3 관련 설정값 가져오기

def trigger_spark_job(credentials):
        """
        SparkSubmitOperator를 통해 Spark job을 실행
        Region 데이터를 변환하고 S3에 적재
        """
    
        spark_submit_task = SparkSubmitOperator(
            task_id="transform_region_data",
            application="/opt/airflow/spark/transform_region_data.py",
            conn_id="spark_default",
            application_args=[
                "--input_s3_path", f"s3a://{S3_BUCKET_NAME}/{RAW_FOLDER}/",
                "--output_s3_path", f"s3a://{S3_BUCKET_NAME}/{PROCESSED_FOLDER}/",
                "--aws_access_key", credentials.access_key,
                "--aws_secret_key", credentials.secret_key,
                "--start_date", "2022-01-02",
                # "--start_date", "2022-08-21",
                "--end_date", "2023-06-03",
                # "--end_date", "2022-08-28",
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

        return spark_result