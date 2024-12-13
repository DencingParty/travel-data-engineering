import numpy as np
import pandas as pd
import os
import shutil
import copy
from tqdm import tqdm

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
    base_path = os.path.join("data", dir1, dir2, "total_combined", "region_data")

    dataset_list = os.listdir(base_path)

    region_dfs = {}
    # region_gps_dfs = {}

    # gps_data, region_data 폴더 내부를 for loop으로 들어간다
    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)
        # dataset_list = os.listdir(f_path)

        # for dataset in dataset_list:
        #     file_path = os.path.join(folder_path, dataset)

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

            print(f"데이터셋: {dataset}\nYMD 컬럼: {ymd_columns}\n")

            # YMD 컬럼 처리
            for ymd_col in ymd_columns:
                df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")

            # 날짜 필터링 (ymd_columns 중 첫 번째 컬럼)
            filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]

            # df_name = dataset.replace(".csv", "")

            # if folder == "gps_data":
            #     region_gps_dfs[df_name] = filtered_df
            # else:
            region_dfs[df_name] = filtered_df

        except pd.errors.EmptyDataError:
            print(f"파일 스킵: {dataset} (비어 있는 파일)")
        except Exception as e:
            print(f"파일 처리 오류: {dataset}: {e}")

    print(f"===================== {dir2} 데이터 필터링 완료 ({start_date} ~ {end_date}) =====================\n")

    # print(f"===================== 데이터 필터링 완료 (시작 날짜 {start_date}) =====================")
    return region_dfs

# gps 데이터 필터링 함수
def filter_by_date_gps(df, ymd_col="DT_YMD", start_date="2022-06-01", end_date="2022-06-06"):

    df[ymd_col] = pd.to_datetime(df[ymd_col], errors="coerce")
    filtered_df = df[(df[ymd_col] >= start_date) & (df[ymd_col] <= end_date)]

    return filtered_df


filtered_dfs_22 = filter_by_date_region("aihub", "2022", "2022-08-10", "2022-09-09")
filtered_gps_dfs_22 = filter_by_date_gps(gps_22, "DT_YMD", "2022-08-10", "2022-09-09")