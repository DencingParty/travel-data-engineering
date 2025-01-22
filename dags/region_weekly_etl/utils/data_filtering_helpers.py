import os
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Region 데이터 필터링 함수
def filter_by_date_region(dir1="aihub", dir2="2023", start_date="2023-06-04", end_date="2023-06-10"):
    base_path = os.path.join("/opt/airflow/data", dir1, dir2, "total_combined", "region_data_parquet")
    
    try:
        dataset_list = [f for f in os.listdir(base_path) if f.endswith('.parquet')]
    except FileNotFoundError:
        logger.error(f"디렉토리를 찾을 수 없습니다: {base_path}")
        return {}

    region_dfs = {}
    start_date = pd.to_datetime(start_date).date()
    end_date = pd.to_datetime(end_date).date()

    for dataset in dataset_list:
        dataset_path = os.path.join(base_path, dataset)

        try:
            df = pd.read_parquet(dataset_path)
            if df.empty:
                logger.warning(f"데이터프레임이 비어 있습니다: {dataset}")
                continue

            df_name = dataset.replace(".parquet", "")
            
            # 날짜 및 시간 컬럼 필터링
            ymd_columns = [col for col in df.columns if col.endswith('YMD') and "START" not in col]
            min_columns = [col for col in df.columns if col.endswith('MIN')]

            # 날짜 및 시간 변환
            for col in ymd_columns + min_columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").astype('datetime64[ms]')  # 밀리초로 변환
                if col in ymd_columns:  # 날짜만 유지
                    df[col] = df[col].dt.date  # datetime.date 객체로 변환

            # 날짜 필터링
            if ymd_columns:
                if df_name.startswith("tn_traveller_master"):
                    filtered_df = df
                    region_dfs[df_name] = filtered_df
                    continue
                logger.info(f"{df_name}의 필터링 기준 컬럼: {ymd_columns[0]}")
                filtered_df = df[(df[ymd_columns[0]] >= start_date) & (df[ymd_columns[0]] <= end_date)]
            else:
                logger.info(f"{df_name}에는 YMD 컬럼이 없습니다.")
                filtered_df = df

            region_dfs[df_name] = filtered_df

        except Exception as e:
            logger.error(f"{dataset} 파일 처리 중 오류가 발생했습니다: {e}")
            continue

    return region_dfs