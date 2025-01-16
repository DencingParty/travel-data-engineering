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

def get_spark_session(aws_access_key, aws_secret_key):
    """
    SparkSession을 생성하거나 기존 세션을 가져옴
    - AWS S3와의 연결 설정 포함
    """
    spark = SparkSession.builder \
        .appName("Transform Region Data") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Spark 로그는 WARN 이상만 출력
    return spark


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
    # 이동내역 (START_VISIT_AREA_ID, START_DT_YMD 제거)
    "tn_move_his": [
        "TRAVEL_ID", "TRIP_ID", "END_VISIT_AREA_ID", 
        "END_DT_YMD", "MVMN_CD_1"
    ],
    # 이동수단소비내역 (PAYMENT_DT_MIN 제거)
    "tn_mvmn_consume": [
        "TRAVEL_ID", "MVMN_SE_NM", "PAYMENT_AMT_WON", "PAYMENT_DT_YMD"
    ],
    "tn_travel_travel": [
        "TRAVEL_ID", "TRAVEL_START_YMD", "TRAVEL_END_YMD", "TRAVEL_NM"
    ],
    "tn_traveller_master": [
        "TRAVEL_ID", "TRAVELER_ID", "RESIDENCE_SGG_CD", "TRAVEL_COMPANIONS_NUM", "GENDER", 
        "AGE_GRP", "EDU_NM", "JOB_NM", "TRAVEL_TERM", "TRAVEL_NUM", 
        "INCOME", "HOUSE_INCOME", "TRAVEL_STYL_1", "TRAVEL_STYL_2", "TRAVEL_STYL_3",
        "TRAVEL_STATUS_START_YMD", "TRAVEL_STATUS_END_YMD"
    ],
    # 방문지정보 (SGG_CD, RESIDENCE_TIME_MIN 제거)
    "tn_visit_area": [
        "TRAVEL_ID", "VISIT_AREA_ID", "VISIT_AREA_NM", "ROAD_NM_ADDR", 
        "LOTNO_ADDR", "X_COORD", "Y_COORD", 
        "RCMDTN_INTENTION", "REVISIT_INTENTION", "DGSTFN", "VISIT_END_YMD"
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
    if "tn_travel_" in base_name:
        base_name = "tn_travel_travel"
    logger.info(f"{base_name} 파일 전처리 시작...")
    
    # 필요 없는 파일 제거
    if base_name in ["tn_adv_consume", "tn_companion_info", "tn_tour_photo"]:
        logger.info(f"{file_name} 사용하지 않는 테이블로 삭제 처리.")
        return None  # None 반환 시 저장하지 않음

    # 메타데이터 (코드A, 코드B, 시군구코드)인 경우, 전처리 작업 스킵
    if base_name.startswith("tc"):
        return df

    # 필요한 컬럼만 남기기
    columns_to_keep = column_selection.get(base_name, [])
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

def check_file_is_in_directory(processed_file_name, bucket_name, output_dir, aws_access_key, aws_secret_key):
    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    # output_dir 내부에 동일한 파일이 이미 존재하는지 확인
    existing_files = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=output_dir.replace(f's3a://{bucket_name}/', '')
    )

    if 'Contents' in existing_files:
        # 파일 이름 매칭 확인
        existing_file_names = [os.path.basename(obj['Key']) for obj in existing_files['Contents']]

        if processed_file_name in existing_file_names:
            logger.info(f"{processed_file_name}는 이미 {output_dir} 경로에 존재합니다. 작업을 스킵합니다.")
            return True  # 작업 스킵
    return False


def backup_existing_files(s3, bucket_name, output_dir, backup_dir):
    """
    기존 _snappy.parquet 파일을 백업 디렉토리로 이동.
    """
    # output_dir에 저장된 기존 파일 리스트 가져오기
    existing_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=output_dir)

    for obj in existing_files.get('Contents', []):
        # _snappy.parquet 파일 필터링
        if obj['Key'].endswith('_snappy.parquet'):
            clean_key = obj['Key']  # 현재 파일의 경로
            backup_key = f"{backup_dir}{os.path.basename(obj['Key'])}" # 백업 디렉토리에 저장할 경로

            # 파일 복사 (현재 경로 → 백업 디렉토리)
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': clean_key}, # 원본 파일 경로
                Key=backup_key # 복사 대상 경로
            )
            logger.info(f" (1) {backup_key}에 snappy 파일 백업 완료")
            # 원본 삭제
            s3.delete_object(Bucket=bucket_name, Key=clean_key)


def merge_and_store_files(s3, bucket_name, temp_dir, output_dir, processed_file_name):
    """
    Spark 출력 파일(part-*)을 최종 _snappy.parquet로 병합 및 저장.
    """
    # temp_dir에 저장된 Spark 출력 파일 리스트
    uploaded_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_dir)

    for obj in uploaded_files.get('Contents', []):
        # Spark의 출력 파일(part-)만 필터링
        if 'part-' in os.path.basename(obj['Key']) and obj['Key'].endswith('.parquet'):
            clean_key = obj['Key'] # 현재 파일의 경로
            final_output_path = f"{output_dir}{processed_file_name}" # 최종 저장 경로

            # 파일 복사 (현재 경로 → 최종 디렉토리)
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': clean_key},  # 원본 파일 경로
                Key=final_output_path # 복사 대상 경로
            )
            logger.info(f" (2) {final_output_path}에 최종 저장 완료!")

            # 원본 part- 파일 삭제
            s3.delete_object(Bucket=bucket_name, Key=clean_key)
            logger.info(f" (2) {clean_key} part- 파일 삭제 완료")


def restore_backup_files(s3, bucket_name, backup_dir, output_dir):
    """
    백업된 _snappy.parquet 파일을 원래 위치로 복원.
    """
    # backup_dir에 저장된 백업 파일 리스트
    backup_files = s3.list_objects_v2(Bucket=bucket_name, Prefix=backup_dir)
    for obj in backup_files.get('Contents', []):
        clean_key = obj['Key'] # 현재 백업 파일의 경로
        restore_key = f"{output_dir}{os.path.basename(obj['Key'])}" # 복원 대상 경로

        # 파일 복사 (백업 디렉토리 → 원래 디렉토리)
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': clean_key}, # 원본 파일 경로
            Key=restore_key # 복사 대상 경로
        )
        logger.info(f" (3) {backup_dir}에서 {restore_key}로 {clean_key} 파일 복사 완료")
        # 백업 디렉토리에서 원본 파일 삭제
        s3.delete_object(Bucket=bucket_name, Key=clean_key)


def process_parquet_file(spark, file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key, meta=False):
    """
    개별 Parquet 파일 transform 및 단일 파일로 S3 저장
    """
    try:
        # 파일명 추출 및 저장 경로 설정
        file_name = os.path.basename(file_path)
        processed_file_name = file_name.replace('.parquet', '_snappy.parquet')

        # S3 클라이언트 초기화  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        if meta:
            output_dir = f"{output_s3_path}metadata/"
            
            # 파일 존재 여부 확인
            if check_file_is_in_directory(processed_file_name, bucket_name, output_dir, aws_access_key, aws_secret_key):
                return  # 파일이 이미 존재하면 작업 종료
        
        else:
            file_prefix = '/'.join(file_path.split('/')[-3:-1])  # 예: 230521/region_data
            output_dir = f"{output_s3_path}{file_prefix}/"
        
        backup_dir = f"{output_dir}snappy-backup/"
        temp_dir = f"{output_dir}temp/"
        
        # 파일 읽기
        logger.info(f"{file_path} 읽는 중...")
        
        df = spark.read.parquet(file_path)
        file_name = os.path.basename(file_path)
        
        # 데이터 전처리
        df = preprocess_data(df, file_name)

        # 데이터가 없거나 비어있는 경우 처리 건너뜀
        if df is None or df.rdd.isEmpty():
            logger.warning(f"{file_name} - 처리할 데이터가 없습니다. 건너뜀.")
            return  # 파일을 저장하지 않고 스킵

        # BINARY 타입 변환
        df = convert_binary_columns(df)

        # 변환 후 데이터 확인
        logger.info(f"{file_path} - 변환 후 데이터 확인:")
        df.show(5, truncate=False)

        # 기존 _snappy.parquet 파일을 snappy-backup/로 이동
        backup_existing_files(s3, bucket_name, output_dir.replace(f's3a://{bucket_name}/', ''), backup_dir)

        # Spark DataFrame을 S3의 temp_dir에 병합 (기존 snappy 파일이 없는 상태)
        logger.info(f"{temp_dir}에 데이터 병합 중...")
        df.coalesce(1).write.mode("overwrite").parquet(temp_dir)
        
        # temp_dir에서 Spark 출력 파일(part-)을 병합하여 최종 _snappy.parquet 파일로 저장.
        merge_and_store_files(
            s3, bucket_name, 
            temp_dir.replace(f's3a://{bucket_name}/', ''), 
            output_dir.replace(f's3a://{bucket_name}/', ''), 
            processed_file_name
        )

        # 백업된 _snappy.parquet 파일을 원래 위치로 복원
        restore_backup_files(s3, bucket_name, backup_dir, output_dir.replace(f's3a://{bucket_name}/', ''))
        logger.info(f"최종 저장된 S3 경로: {output_dir}")

    except AnalysisException as e:
        logger.error(f"파일 읽기 실패: {file_path} - {str(e)}")
    except Exception as e:
        logger.error(f"{file_path} 처리 중 오류 발생: {str(e)}")


def list_s3_parquet_files(bucket_name, prefix, aws_access_key, aws_secret_key):
    """
    S3에서 parquet 파일 목록 가져오기
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)

    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            logger.warning(f"{prefix} 경로에 parquet 파일이 없습니다.")
            return []

        parquet_files = [
            f"s3a://{bucket_name}/{item['Key']}"
            for item in response['Contents']
            if item['Key'].endswith('.parquet')
        ]
        logger.info(f"{prefix} 경로에서 {len(parquet_files)}개의 parquet 파일 발견.")
        return parquet_files

    except Exception as e:
        logger.error(f"S3에서 파일 목록을 가져오는 중 오류 발생: {str(e)}")
        return []


def main(input_s3_path, output_s3_path, aws_access_key, aws_secret_key, start_date, end_date):
    bucket_name = input_s3_path.split('/')[2]
    spark = get_spark_session(aws_access_key, aws_secret_key)
    logger.info(f"데이터 Transform 시작 일자: {start_date} ~ {end_date}")
    
    try:
        # Meta 파일 처리
        meta_target_prefix = f"{input_s3_path.replace(f's3a://{bucket_name}/', '')}metadata/"
        logger.info(f"처리 대상 S3 경로: {meta_target_prefix}")
        meta_parquet_files = list_s3_parquet_files(bucket_name, meta_target_prefix, aws_access_key, aws_secret_key)

        if not meta_parquet_files:
            logger.warning("metadata 경로에 parquet 파일이 없습니다.")
        else:
            for file_path in meta_parquet_files:
                process_parquet_file(spark, file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key, meta=True)

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
                process_parquet_file(spark, file_path, output_s3_path, bucket_name, aws_access_key, aws_secret_key, meta=False)
                
    finally:
        spark.stop()

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