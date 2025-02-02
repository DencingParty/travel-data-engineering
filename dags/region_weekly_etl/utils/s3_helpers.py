import boto3
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import logging
from io import BytesIO
from datetime import datetime, timedelta
from region_weekly_etl.config import S3_BUCKET_NAME, RAW_FOLDER

logger = logging.getLogger(__name__)

# S3 클라이언트 생성 함수
def get_s3_client():
    aws_hook = AwsBaseHook(aws_conn_id='AWS_CONNECTION_ID', client_type='s3')
    credentials = aws_hook.get_credentials()

    aws_conn = BaseHook.get_connection("AWS_CONNECTION_ID")
    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "ap-northeast-2")
    )
    return session.client("s3")

#  S3 버킷에서 지정된 prefix 하위 폴더 중 특정 날짜 이후 데이터를 삭제하는 함수 (metadata 폴더는 제외)
def clean_s3_folder(bucket_name, prefix, start_date="2023-06-04"):
    s3_client = get_s3_client() # S3 클라이언트 가져오기
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    # prefix 하위 객체 목록 가져오기
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # 'Contents' 키가 없을 경우 빈 리스트를 반환하므로 별도 조건문 없이 반복문 실행 가능
    for obj in response.get('Contents', []):     # 'Contents' 키가 없으면 빈 리스트를 반환
        key = obj['Key']
        folder_date_str = key.split('/')[1]

        # 메타데이터라면 바로 continue
        if folder_date_str == "metadata":
            logger.info(f"메타데이터 폴더는 건너뜁니다: {key}.")
            continue

        try:
            # 폴더 이름을 날짜로 변환
            folder_date = datetime.strptime(folder_date_str, "%y%m%d")

            # start_date 이후 데이터인지 확인
            if folder_date >= start_date:
                # S3 객체 삭제
                s3_client.delete_object(Bucket=bucket_name, Key=key)
                logger.info(f"S3에서 {key}를 삭제했습니다.")

        except ValueError:
            # ValueError 예외 처리와 메시지 간결화
            logger.warning(f"날짜 형식이 아닌 폴더를 건너뜁니다: {folder_date_str}")

# S3에 데이터를 업로드
def upload_to_s3(data, start_date, is_gps=False):
    s3_client = get_s3_client()
    
    # 시작 날짜를 변환하여 경로의 일부로 사용 (YYMMDD 형식)
    transformed_key = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    
    # GPS 여부에 따라 base_path를 설정
    base_path = f"{transformed_key}/gps_data" if is_gps else f"{transformed_key}/region_data"

    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")

    try:
        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터를 S3에 업로드합니다 ({base_path})")

        for file_name, df in data.items():

            if is_gps:
                file_name_key = f"gps_data_{transformed_key}"
                s3_key = f"{RAW_FOLDER}/{base_path}/{file_name_key}.parquet"

            else:
                if "tc_" in file_name or "tn_activity_his_" in file_name or "tn_traveller_master_" in file_name:
                    file_name_key = f"{file_name}"
                    s3_key = f"{RAW_FOLDER}/metadata/{file_name_key}.parquet"

                else:
                    file_name_key = f"{file_name}_{transformed_key}"
                    s3_key = f"{RAW_FOLDER}/{base_path}/{file_name_key}.parquet"

            # DataFrame을 Parquet로 변환
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False, coerce_timestamps="ms") 

            # 변환된 Parquet을 S3 업로드
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )

        logger.info(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드가 완료되었습니다.")

    except Exception as e:
        logger.error(f"일주일 ({start_date} ~ {end_date}) 데이터 업로드 중 오류 발생: {e}")