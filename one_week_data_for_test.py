import numpy as np
import pandas as pd
import os
import shutil
import copy
from tqdm import tqdm
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings(action='ignore')
pd.set_option("display.max_columns", None)

# 현재 작업 디렉토리 가져오기
original_directory = os.getcwd()

path_parquet_22 = os.path.join("data", "aihub", "2022", "total_combined", "gps_data_parquet", "total_gps_2022.parquet")
path_parquet_23 = os.path.join("data", "aihub", "2023", "total_combined", "gps_data_parquet", "total_gps_2023.parquet")
gps_22 = pd.read_parquet(path_parquet_22, engine="pyarrow")
gps_23 = pd.read_parquet(path_parquet_23, engine="pyarrow")

region_22 = os.listdir(os.path.join("data", "aihub", "2022", "total_combined", "region_data"))
region_23 = os.listdir(os.path.join("data", "aihub", "2023", "total_combined", "region_data"))

def filter_by_date_region(dir1="aihub", dir2="2022", start_date="2022-06-01", end_date="2022-06-06"):
    
    # 기본 경로 설정 (data\aihub\2022)
    if start_date.startswith("2022"):       
        base_path = os.path.join("data", dir1, dir2, "total_combined", "region_data")
    else:
        dir2 = "2023"
        base_path = os.path.join("data", dir1, dir2, "total_combined", "region_data")

    dataset_list = os.listdir(base_path)

    region_dfs = {}

    # gps_data, region_data 폴더 내부를 for loop으로 들어간다
    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)

        if not dataset.endswith('.csv'):
            print(f"파일 스킵: {dataset} (CSV 파일 아님)")
            continue

        try:
            # 파일 읽기 시도
            df = pd.read_csv(dataset_path)
            df_name = dataset.replace(".csv", "")
            
            # 파일에 데이터가 없는 경우 스킵
            if df.empty:
                print(f"파일 스킵: {dataset} (데이터 없음)")
                continue

            # 데이터프레임에서 "YMD"로 끝나는 컬럼 찾는다 (START_DATE_YMD는 제외)
            ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]

            if not ymd_columns:
                print(f"YMD 컬럼 없음: {dataset}\n")

                # if folder == "gps_data":
                #     region_gps_dfs[df_name] = df
                # else:
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
def filter_by_date_gps(df, ymd_col="DT_YMD", start_date="2022-06-01", end_date="2022-06-06"):

    df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")
    filtered_df = df[(df[ymd_col] >= start_date) & (df[ymd_col] <= end_date)]

    return filtered_df

def get_weekly_region(weekly_dict):
    region_dict = {}
    
    for key in weekly_dict:

        start_date = weekly_dict[key]["start_date"]
        end_date = weekly_dict[key]["end_date"]

        region_dict[start_date] = filter_by_date_region(dir1="aihub", dir2="2022", 
                                                        start_date=start_date, end_date=end_date)
    
    return region_dict

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

# 지역별 데이터 통합, 저장 함수 생성
def store_filtered_data(data_dict, dir1="aihub", is_gps=True):
    
    # 마지막 코드의 날짜 출력용
    data_keys = list(data_dict.keys())
    first_date = data_keys[0]
    last_date = datetime.strptime(data_keys[-1], "%Y-%m-%d") + timedelta(days=6)
    last_date = last_date.strftime("%Y-%m-%d")

    # 기본 경로 설정 (data\aihub\)
    base_path = os.path.join("data", dir1, "filtered")

    for key, value in data_dict.items():

        start_date = key
        start_plus_6 = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)
        end_date = start_plus_6.strftime("%Y-%m-%d")

        # "2022-01-02" → "220102" 형태로 변경
        start_date_formatted = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
        date_path = os.path.join(base_path, start_date_formatted)

        # "220102" 이름의 폴더 생상
        os.makedirs(date_path, exist_ok=True)

        # data_dict가 gps 데이터일 때
        if is_gps:
            if isinstance(data_dict[key], pd.DataFrame):  # df가 데이터프레임인지 확인
                data_count = 1
            else:
                data_count = 0

            folder_path = os.path.join(date_path, "gps_data")
            os.makedirs(folder_path, exist_ok=True)

            parquet_path = os.path.join(folder_path, f"gps_data_{start_date_formatted}.parquet")
            data_dict[key].to_parquet(parquet_path, engine="pyarrow", index=False)
        
        # data_dict가 region 데이터일 때
        else:
            data_count = len(data_dict[key].keys())

            folder_path = os.path.join(date_path, "region_data")
            os.makedirs(folder_path, exist_ok=True)

            for csv_name in data_dict[key].keys():
            
                csv_path = os.path.join(folder_path, f"{csv_name}_{start_date_formatted}.csv")
                # data_dict[key][csv_name].to_parquet(parquet_path, engine="pyarrow", index=False)
                data_dict[key][csv_name].to_csv(csv_path, index=False)
        
        print(f"------------------ 필터링 데이터 {data_count}개 저장 완료 ({start_date} ~ {end_date}) ------------------")

    print(f"\n=================== 필터링 데이터 총 {len(data_dict.keys())}개 저장 완료 ({first_date} ~ {last_date}) ===================")
    print(f"\n=================== 저장 경로: {folder_path} ===================")



# 2022-01-02 ~ 2023-06-03까지 일주일 단위로 딕셔너리 생성
# 일주일 단위의 시작 날짜와 끝 날짜를 포함한 데이터 (이중 딕셔너리)
weekly_default_dict = {
    f"week_{i+1}": {
        "start_date": str(start.date()),
        "end_date": str((start + pd.Timedelta(days=6)).date())
    }
    for i, start in enumerate(pd.date_range(start="2022-01-02", end="2023-06-03", freq="7D"))
}



region_dict_default = get_weekly_region(weekly_default_dict)
gps_dict_default = get_weekly_gps(weekly_default_dict)

store_filtered_data(region_dict_default, dir1="aihub", is_gps=False)
store_filtered_data(gps_dict_default, dir1="aihub", is_gps=True)