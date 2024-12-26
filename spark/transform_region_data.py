from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
import argparse
import logging
import boto3
import os


# 설정 및 Spark 세션 초기화
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Transform Region Data") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

# Spark 내부 로그만 WARN으로 설정
spark.sparkContext.setLogLevel("WARN")  # Spark 로그는 WARN 이상만 출력


def convert_binary_columns(df):
    """
    BINARY 타입으로 인식된 열을 string으로 변환
    """
    for field in df.schema.fields:
        if field.dataType.simpleString() == 'binary':
            logger.warning(f"{field.name} 칼럼이 BINARY로 인식됨. string으로 변환합니다.")
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df


def process_parquet_file(file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key):
    """
    개별 parquet 파일 transform 및 단일 파일로 S3 저장
    """
    try:
        # 파일 읽기
        logger.info(f"{file_path} 읽는 중...")
        df = spark.read.parquet(file_path)
        
        # 원본 데이터 확인
        logger.info(f"{file_path} - 원본 데이터 확인:")
        df.show(5, truncate=False)

        # BINARY 타입 변환
        df = convert_binary_columns(df)

        # 변환 후 데이터 확인
        logger.info(f"{file_path} - 변환 후 데이터 확인:")
        df.show(5, truncate=False)

        # 파일명 추출 및 저장 경로 설정
        file_name = os.path.basename(file_path)
        file_prefix = '/'.join(file_path.split('/')[-3:-1])  # 230521/region_data 추출
        processed_file_name = file_name.replace('.parquet', '_snappy.parquet')
        output_dir = f"{output_s3_path}{file_prefix}/"
        temp_output_path = f"{output_dir}temp/"

        # 저장된 디렉터리에서 파일을 찾아 단일 파일로 복사
        s3 = boto3.client('s3',
                          aws_access_key_id=aws_access_key,
                          aws_secret_access_key=aws_secret_key)
        existing_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=output_dir.replace(f's3a://{bucket_name}/', ''))

        # test log
        logger.info("병합 전 디렉터리 상태:")
        for obj in existing_files.get('Contents', []):
            logger.info(f"S3 파일 목록: {obj['Key']}")

        # snappy 파일을 snappy-backup/로 이동
        for obj in existing_files.get('Contents', []):
            if obj['Key'].endswith('_snappy.parquet'):
                # s3://, s3a:// 제거
                clean_key = obj['Key']
                backup_key = f"{output_dir.replace(f's3a://{bucket_name}/', '')}snappy-backup/{os.path.basename(obj['Key'])}"
                
                # 복사 수행
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': clean_key},
                    Key=backup_key
                )

                # 로그로 복사 대상과 복사 후 경로 확인
                logger.info(f"CopySource: {clean_key} -> {backup_key}")

                # 원본 삭제 (s3:// 제거된 경로 사용)
                s3.delete_object(Bucket=bucket_name, Key=clean_key)
                logger.info(f"{clean_key} snappy 파일 임시 이동 완료")

        # test log
        logger.info("snappy 파일 임시 이동 후 디렉터리 상태:")
        for obj in existing_files.get('Contents', []):
            logger.info(f"S3 파일 목록: {obj['Key']}")

        # 새로운 데이터 병합 (기존 snappy 파일이 없는 상태)
        logger.info(f"{temp_output_path}에 데이터 저장 중...")
        df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)
        
        # boto3에서는 's3a://' 제거
        uploaded_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_output_path.replace(f's3a://{bucket_name}/', ''))

        # test log
        logger.info("병합 후 디렉터리 상태:")
        for obj in uploaded_files.get('Contents', []):
            logger.info(f"S3 파일 목록: {obj['Key']}")

        # part-로 시작하는 파일 복사 및 삭제
        for obj in uploaded_files.get('Contents', []):
            if 'part-' in os.path.basename(obj['Key']) and obj['Key'].endswith('.parquet'):
                # s3://, s3a:// 제거
                clean_key = obj['Key']
                final_output_path = f"{output_dir.replace(f's3a://{bucket_name}/', '')}{processed_file_name}"

                # 복사 수행
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': clean_key},
                    Key=final_output_path
                )
                logger.info(f"{final_output_path}에 최종 저장 완료!")

                # 원본 part- 파일 삭제
                s3.delete_object(Bucket=bucket_name, Key=clean_key)
                logger.info(f"{clean_key} part- 파일 삭제 완료")

        # 기존 snappy 파일 복원
        backup_files = s3.list_objects_v2(
            Bucket=bucket_name, 
            Prefix=f"{output_dir.replace(f's3a://{bucket_name}/', '')}snappy-backup/"
        )
        for obj in backup_files.get('Contents', []):
            clean_key = obj['Key']
            restore_key = f"{output_dir.replace(f's3a://{bucket_name}/', '')}{os.path.basename(obj['Key'])}"

            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': clean_key},
                Key=restore_key
            )
            s3.delete_object(Bucket=bucket_name, Key=clean_key)
            logger.info(f"{clean_key} snappy 파일 복원 완료")

        # test log
        final_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=output_dir.replace(f's3a://{bucket_name}/', ''))
        logger.info("최종 디렉터리 상태:")
        for obj in final_files.get('Contents', []):
            logger.info(f"S3 파일 목록: {obj['Key']}")

    except AnalysisException as e:
        logger.error(f"파일 읽기 실패: {file_path} - {str(e)}")
    except Exception as e:
        logger.error(f"{file_path} 처리 중 오류 발생: {str(e)}")


def list_s3_parquet_files(bucket_name, prefix, aws_access_key, aws_secret_key):
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' not in response:
        logger.warning(f"{prefix} 경로에 parquet 파일이 없습니다.")
        return []

    parquet_files = [
        f"s3a://{bucket_name}/{item['Key']}"
        for item in response['Contents']
        if item['Key'].endswith('.parquet')
    ]
    return parquet_files


def main(input_s3_path, output_s3_path, aws_access_key, aws_secret_key):
    bucket_name = input_s3_path.split('/')[2]
    prefix = '/'.join(input_s3_path.split('/')[3:])

    # 230521/region_data 디렉토리만 처리하도록 경로 수정
    target_prefix = f"{prefix}230521/region_data/"
    logger.info(f"처리 대상 S3 경로: {target_prefix}")

    # boto3로 parquet 파일 목록 조회
    parquet_files = list_s3_parquet_files(bucket_name, target_prefix, aws_access_key, aws_secret_key)

    if not parquet_files:
        logger.warning("처리할 parquet 파일이 없습니다.")
        return

    # 파일별 transform 처리
    for file_path in parquet_files:
        process_parquet_file(file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_s3_path", required=True)
    parser.add_argument("--output_s3_path", required=True)
    parser.add_argument("--aws_access_key", required=True)
    parser.add_argument("--aws_secret_key", required=True)

    args = parser.parse_args()
    main(args.input_s3_path, args.output_s3_path, args.aws_access_key, args.aws_secret_key)

    # Spark 세션 종료
    spark.stop()