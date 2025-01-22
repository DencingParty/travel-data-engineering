import boto3
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from io import BytesIO
from datetime import datetime, timedelta
import logging

from region_initial_etl.config import S3_BUCKET_NAME, RAW_FOLDER

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
