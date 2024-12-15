import numpy as np
import pandas as pd
import os
import shutil
import copy
from tqdm import tqdm
from datetime import datetime, timedelta

import boto3
from io import BytesIO

import warnings
warnings.filterwarnings(action='ignore')
pd.set_option("display.max_columns", None)

"""
Region, GPS 데이터 필터링 함수
- start_date, end_date를 통해 기간 설정 → 해당 기간에 속하는 데이터만 필터링

filter_by_date_region: Dictionary 파일 형태 (key: csv 파일명, value: DataFrame)
    - e.g. filtered_dfs_22["tn_mvmn_consume_his_이동수단소비내역"]

filter_by_date_gps: 기간으로 필터링한 DataFrame
    - e.g. filtered_gps_dfs_22
"""

# Region 데이터 필터링 함수
def filter_by_date_region(dir1="aihub", dir2="2022", start_date="2022-01-02", end_date="2022-01-08"):
    
    # 기본 경로 설정 (data\aihub\2022)
    if start_date.startswith("2022"):       
        base_path = os.path.join("data", dir1, dir2, "total_combined", "region_data_parquet")
    else:
        dir2 = "2023"
        base_path = os.path.join("data", dir1, dir2, "total_combined", "region_data_parquet")

    dataset_list = os.listdir(base_path)

    region_dfs = {}

    # gps_data_parquet, region_data_parquet 폴더 내부를 for loop으로 들어간다
    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)

        if not dataset.endswith('.parquet'):
            print(f"파일 스킵: {dataset} (parquet 파일 아님)")
            continue

        try:
            # 파일 읽기 시도
            df = pd.read_parquet(dataset_path, engine="pyarrow")
            df_name = dataset.replace(".parquet", "")
            
            # 파일에 데이터가 없는 경우 스킵
            if df.empty:
                print(f"파일 스킵: {dataset} (데이터 없음)")
                continue

            # 데이터프레임에서 "YMD"로 끝나는 컬럼 찾는다 (START_DATE_YMD는 제외)
            ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]

            if not ymd_columns:
                print(f"YMD 컬럼 없음: {dataset}")

                region_dfs[df_name] = df
                
                continue
            
            if len(ymd_columns) > 1:
                ymd_columns = [ymd_columns[-1]]

            # print(f"데이터셋: {dataset}\nYMD 컬럼: {ymd_columns}\n")

            # YMD 컬럼 처리
            for ymd_col in ymd_columns:
                df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")

            # 날짜 필터링 (ymd_columns 중 첫 번째 컬럼)
            filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]

            region_dfs[df_name] = filtered_df

        except pd.errors.EmptyDataError:
            print(f"파일 스킵: {dataset} (비어 있는 파일)")
        except Exception as e:
            print(f"파일 처리 오류: {dataset}: {e}")

    print(f"===================== {dir2} 데이터 필터링 완료 ({start_date} ~ {end_date}) =====================\n")

    return region_dfs

# gps 데이터 필터링 함수
def filter_by_date_gps(df, ymd_col="DT_YMD", start_date="2022-01-02", end_date="2022-01-08"):

    df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")
    filtered_df = df[(df[ymd_col] >= start_date) & (df[ymd_col] <= end_date)]

    print(f"===================== gps 데이터 필터링 완료 ({start_date} ~ {end_date}) =====================\n")

    return filtered_df


"""
set_filtering_date
- 2022-01-02 ~ 2023-06-03까지 일주일 단위로 Dictionary 생성 (초기 값)
    - 딕셔너리 예시 (Nested Dictionary)
        - weekly_dict["week_1"]["start_date"] = '2022-01-02'
        - weekly_dict["week_1"]["end_date"] = '2022-01-08'
- 일주일 단위의 시작 날짜와 끝 날짜를 포함한 데이터
- S3에 한 번만 넣어준다

get_weekly_region
- Nested Dictionary (첫 번째 key: 매 주의 시작 날짜, 두 번째 key: csv 파일명, value: DataFrame)
    - e.g. region_dict_default["2023-05-14"]["tn_activity_consume_his_활동소비내역"] (2023-05-14 ~ 2023-05-20)
- YMD 컬럼 (날짜) 없는 csv는 기간 필터링 없이 전체를 불러온다
    - e.g. region_dict_default["2023-05-14"]["tn_activity_his_활동내역"]

get_weekly_gps
- Dictionary (key: 매 주의 시작 날짜, value: DataFrame)
    - e.g. gps_dict_default["2022-10-16"]
"""

def set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D"):
    weekly_dict = {
        f"week_{i+1}": {
            "start_date": str(start.date()),
            "end_date": str((start + pd.Timedelta(days=6)).date())
        }
        for i, start in enumerate(pd.date_range(start=start_date, end=end_date, freq=freq))
    }

    return weekly_dict

# 2022-01-02 ~ 2023-06-03 필터링 기간 설정 (초기 값)
weekly_default_dict = set_filtering_date(start_date="2022-01-02", end_date="2023-06-03", freq="7D")

# Region 데이터 초기 값 세팅
def get_weekly_region(weekly_dict):
    region_dict = {}
    
    for key in weekly_dict:

        start_date = weekly_dict[key]["start_date"]
        end_date = weekly_dict[key]["end_date"]

        region_dict[start_date] = filter_by_date_region(dir1="aihub", dir2="2022", 
                                                        start_date=start_date, end_date=end_date)
    
    return region_dict

# GPS 데이터 초기 값 세팅
def get_weekly_gps(weekly_dict):

    # gps 파일 불러온다
    path_parquet_22 = os.path.join("data", "aihub", "2022", "total_combined", 
                                   "gps_data_parquet", "total_gps_2022.parquet")
    path_parquet_23 = os.path.join("data", "aihub", "2023", "total_combined", 
                                   "gps_data_parquet", "total_gps_2023.parquet")
    
    gps_22 = pd.read_parquet(path_parquet_22, engine="pyarrow")
    gps_23 = pd.read_parquet(path_parquet_23, engine="pyarrow")

    gps_dict = {}

    # week_53에서 연도 바뀐다 (2023년)
    for key in weekly_dict:
        start_date = weekly_dict[key]["start_date"]
        end_date = weekly_dict[key]["end_date"]

        if start_date.startswith("2022"):
            gps_dict[start_date] = filter_by_date_gps(gps_22, "DT_YMD", start_date, end_date)

        else:
            gps_dict[start_date] = filter_by_date_gps(gps_23, "DT_YMD", start_date, end_date)
    
    return gps_dict

# 초기 값 dictionary를 기반으로 각각의 함수 실행
region_dict_default = get_weekly_region(weekly_default_dict)
gps_dict_default = get_weekly_gps(weekly_default_dict)


# S3 클라이언트 생성
s3_client = boto3.client('s3')

# S3 버킷 이름과 폴더 설정
bucket_name = 'travel-de-storage'
s3_folder = 'raw-data'

# 날짜 형식 변환 함수
def transform_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%y%m%d")  # 날짜를 220102 형식으로 변환
    except ValueError:
        return date_str  # 날짜 형식이 아니면 그대로 반환

# DataFrame을 S3에 업로드하는 함수
def upload_to_s3(data, is_gps, current_path=""):
    for key, value in data.items():
        # 현재 경로 업데이트
        transformed_key = transform_date(key)
        if is_gps:
            base_path = f"{transformed_key}/gps_data"
            file_name = f"gps_data_{transformed_key}"
        else:
            base_path = f"{transformed_key}/region_data"
            file_name = f"{base_path.split('/')[-1]}_{transformed_key}"

        s3_key = f"{s3_folder}/{base_path}/{file_name}.parquet"

        if isinstance(value, pd.DataFrame):  # 값이 DataFrame인 경우
            # DataFrame을 Parquet로 변환
            parquet_buffer = BytesIO()
            value.to_parquet(parquet_buffer, index=False)

            # S3 업로드
            try:
                print(f"--------------------- S3에 DataFrame 적재중 {s3_key} ---------------------")
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=parquet_buffer.getvalue()
                )
                print(f"--------------------- S3에 DataFrame 적재 완료! {s3_key} ---------------------\n")
            except Exception as e:
                print(f"--------------------- S3에 적재 중 에러 발생 {s3_key}: {e} ---------------------\n")

        elif isinstance(value, dict):  # 값이 딕셔너리인 경우 (이중 딕셔너리 처리)
            for sub_key, sub_value in value.items():
                # 하위 딕셔너리의 파일 경로 설정
                if is_gps:
                    sub_file_path = f"{transformed_key}/gps_data"
                    sub_file_name = f"gps_data_{transformed_key}"
                else:
                    sub_file_path = f"{transformed_key}/region_data"
                    sub_file_name = f"{sub_key}_{transformed_key}"

                sub_s3_key = f"{s3_folder}/{sub_file_path}/{sub_file_name}.parquet"

                if isinstance(sub_value, pd.DataFrame):  # 이중 딕셔너리의 값이 DataFrame인 경우
                    # DataFrame을 Parquet로 변환
                    parquet_buffer = BytesIO()
                    sub_value.to_parquet(parquet_buffer, index=False)

                    # S3 업로드
                    try:
                        print(f"--------------------- S3에 DataFrame 적재중 {sub_s3_key} ---------------------")
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=sub_s3_key,
                            Body=parquet_buffer.getvalue()
                        )
                        print(f"--------------------- S3에 DataFrame 적재 완료! {sub_s3_key} ---------------------\n")
                    except Exception as e:
                        print(f"--------------------- S3에 적재 중 에러 발생 {sub_s3_key}: {e} ---------------------\n")
                else:
                    print(f"--------------------- Data가 Nested Dictionary가 아닙니다: {type(sub_value)} ---------------------\n")
        else:
            print(f"--------------------- 지원하지 않는 Data Type {type(value)} ---------------------\n")
    print(f"===================== S3에 모든 데이터 적제 완료! {s3_folder} =====================\n")

# region 데이터 S3 적재
upload_to_s3(region_dict_default, is_gps=False)

# region 데이터 S3 적재
upload_to_s3(gps_dict_default, is_gps=True)