from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
import argparse
import logging
import boto3
import os
import pandas as pd


# 설정 및 Spark 세션 초기화
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Transform Region Data") \
    .config("spark.sql.legacy.parquet.int64AsTimestampMillis", "false") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

# Spark 내부 로그만 WARN으로 설정
spark.sparkContext.setLogLevel("WARN")  # Spark 로그는 WARN 이상만 출력

column_selection = {
    # 활동소비내역 (PAYMENT_DT_MIN 제거, SGG_CD 제거)
    "tn_activity_consume": [
        "TRAVEL_ID", "VISIT_AREA_ID", "ACTIVITY_TYPE_CD", 
        "PAYMENT_AMT_WON", "PAYMENT_DT_YMD", "STORE_NM"
    ],
    # 활동내역 (ACTIVITY_ETC 제거, ACTIVITY_DTL,EXPND_SE 결측치 많음)
    "tn_activity_his": [
        "ACTIVITY_TYPE_CD", "ACTIVITY_DTL", 
        "EXPND_SE", "TRAVEL_ID", "VISIT_AREA_ID"
    ],
    # 숙박소비내역 (ROAD_NM_ADDR 결측치 많음, LOTNO_ADDR 제거, PAYMENT_DT_MIN 제거)
    "tn_lodge_consume": [
        "TRAVEL_ID", "LODGING_NM", "ROAD_NM_ADDR", 
        "PAYMENT_AMT_WON", "PAYMENT_DT_YMD"
    ],
    # 이동내역
    "tn_move_his": [
        "TRAVEL_ID", "TRIP_ID", "START_VISIT_AREA_ID", "END_VISIT_AREA_ID", 
        "START_DT_YMD", "START_DT_MIN", "END_DT_YMD", "END_DT_MIN", 
        "MVMN_CD_1", "MVMN_CD_2"
    ],
    # 이동수단소비내역 (PAYMENT_DT_MIN 제거)
    "tn_mvmn_consume": [
        "TRAVEL_ID", "MVMN_SE_NM", "PAYMENT_AMT_WON", "PAYMENT_DT_YMD"
    ],
    "tn_travel_여행": [
        "TRAVEL_ID", "TRAVEL_START_YMD", "TRAVEL_END_YMD", "TRAVEL_NM"
    ],
    "tn_traveller_master": [
        "TRAVELER_ID", "RESIDENCE_SGG_CD", "TRAVEL_COMPANIONS_NUM", "GENDER", 
        "AGE_GRP", "EDU_NM", "JOB_NM", "TRAVEL_TERM", "TRAVEL_NUM", 
        "INCOME", "HOUSE_INCOME", "TRAVEL_STYL_1", "TRAVEL_STYL_2", "TRAVEL_STYL_3"
    ],
    # 방문지정보 (SGG_CD, RESIDENCE_TIME_MIN 제거)
    "tn_visit_area": [
        "TRAVEL_ID", "VISIT_AREA_ID", "VISIT_AREA_NM", "ROAD_NM_ADDR", 
        "LOTNO_ADDR", "X_COORD", "Y_COORD", 
        "RCMDTN_INTENTION", "REVISIT_INTENTION", "DGSTFN"
    ]
}

def convert_binary_columns(df):
    """
    BINARY 타입으로 인식된 열을 string으로 변환
    """
    for field in df.schema.fields:
        if field.dataType.simpleString() == 'binary':
            logger.warning(f"{field.name} 칼럼이 BINARY로 인식됨. string으로 변환합니다.")
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df

def preprocess_data(df: DataFrame, file_name: str) -> DataFrame:
    """
    데이터 전처리 작업
    - 파일명에 따라 필요한 컬럼만 추출
    - 결측치가 있는 행 제거
    """
    base_name = "_".join(file_name.split("_")[:3])  # 파일명에서 처음 3개 단어 추출
    logger.info(f"{base_name} 파일 전처리 시작...")
    
    # 필요 없는 파일 제거
    if base_name in ["tn_adv_consume", "tn_companion_info", "tn_tour_photo"]:
        logger.info(f"{file_name} 사용하지 않는 테이블로 삭제 처리.")
        return None  # None 반환 시 저장하지 않음
    
    # 필요한 컬럼만 남기기
    if base_name in column_selection:
        columns_to_keep = column_selection[base_name]
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        
        if not existing_columns:
            logger.warning(f"{file_name}에서 필요한 컬럼이 존재하지 않습니다.")
            return None
        
        df = df.select(*existing_columns)
        logger.info(f"{file_name}에서 {len(existing_columns)}개 컬럼 유지, 나머지 컬럼 제거.")

        # 결측치 처리
        before_drop = df.count()
        df = df.na.drop()
        after_drop = df.count()
        dropped_rows = before_drop - after_drop
        logger.info(f"{file_name}에서 {before_drop}개 행 중 {dropped_rows}개의 결측치 행 제거.")

    return df

def process_parquet_file(file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key):
    """
    개별 parquet 파일 transform 및 단일 파일로 S3 저장
    """
    try:
        # 파일 읽기
        logger.info(f"{file_path} 읽는 중...")
        
        df = spark.read.parquet(file_path)
        file_name = os.path.basename(file_path)
        
        # 원본 데이터 확인
        logger.info(f"{file_path} - 원본 데이터 확인:")
        df.show(5, truncate=False)

        # 데이터 전처리 추가
        df = preprocess_data(df, file_name)

        if df is None:
            logger.info(f"{file_path} - 저장 대상이 아님. 처리 건너뜀.")
            return  # 파일을 저장하지 않고 스킵

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

                # 원본 삭제 (s3:// 제거된 경로 사용)
                s3.delete_object(Bucket=bucket_name, Key=clean_key)

        # 새로운 데이터 병합 (기존 snappy 파일이 없는 상태)
        logger.info(f"{temp_output_path}에 데이터 저장 중...")
        df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)
        
        # boto3에서는 's3a://' 제거
        uploaded_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_output_path.replace(f's3a://{bucket_name}/', ''))

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


def main(input_s3_path, output_s3_path, aws_access_key, aws_secret_key, start_date, end_date):
    bucket_name = input_s3_path.split('/')[2]
    
    # 주 단위 날짜 생성
    date_range = pd.date_range(start=start_date, end=end_date, freq='7D').strftime("%y%m%d")

    for date in date_range:
        target_prefix = f"{input_s3_path.replace(f's3a://{bucket_name}/', '')}{date}/region_data/"
        logger.info(f"처리 대상 S3 경로: {target_prefix}")

        parquet_files = list_s3_parquet_files(bucket_name, target_prefix, aws_access_key, aws_secret_key)

        if not parquet_files:
            logger.warning(f"{date} - 처리할 parquet 파일이 없습니다.")
            continue

        for file_path in parquet_files:
            process_parquet_file(file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_s3_path", required=True)
    parser.add_argument("--output_s3_path", required=True)
    parser.add_argument("--aws_access_key", required=True)
    parser.add_argument("--aws_secret_key", required=True)
    parser.add_argument("--start_date", required=True)
    parser.add_argument("--end_date", required=True)

    args = parser.parse_args()
    main(args.input_s3_path, args.output_s3_path, args.aws_access_key, args.aws_secret_key, args.start_date, args.end_date)

    # Spark 세션 종료
    spark.stop()