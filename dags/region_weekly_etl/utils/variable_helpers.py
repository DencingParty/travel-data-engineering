from airflow.models import Variable
from datetime import datetime, timedelta
from region_weekly_etl.config import START_DATE, CURRENT_DATE, RUN_STATE_FLAG  # config.py에서 가져오기

# 변수를 동적으로 가져오는 함수
def get_airflow_variable(name, default=None):
    """
    주어진 이름의 Airflow 변수를 가져옵니다. 없으면 기본값을 반환합니다.
    :param name: Airflow 변수 이름
    :param default: 기본값 (변수가 없을 경우 반환)
    :return: 변수 값 또는 기본값
    """
    try:
        return Variable.get(name, default_var=default)
    except KeyError:
        return default

# DAG 실행을 위한 컨텍스트 반환 함수
def get_task_context(start_date=START_DATE):
    """
    DAG 실행을 위한 컨텍스트를 반환합니다.
    Airflow에서 컨텍스트: 태스크 실행과 관련된 정보의 집합 (DAG 실행 정보, 환경 변수, 사용자 정의 데이터 등)
    """
    # config.py에서 가져온 값 사용
    run_state = get_airflow_variable(RUN_STATE_FLAG, default="pending")  # 기본값은 "pending"
    
    # CURRENT_DATE는 문자열로 가정, 필요 시 변환
    if isinstance(CURRENT_DATE, str):
        forced_date = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
    else:
        forced_date = CURRENT_DATE
    
    # start_date가 datetime인지 확인하고 문자열로 변환
    if isinstance(start_date, datetime):
        start_date_str = start_date.strftime("%Y-%m-%d")
    else:
        start_date_str = start_date

    # 시작일을 강제 날짜를 기준으로 주 단위로 계산
    start_date = (
        datetime.strptime(start_date_str, "%Y-%m-%d") +
        timedelta(
            weeks=(
                (datetime.strptime(forced_date.strftime("%Y-%m-%d"), "%Y-%m-%d") -
                 datetime.strptime(start_date_str, "%Y-%m-%d")
                ).days // 7
            ) - 1
        )
    ).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
    next_date = (forced_date + timedelta(weeks=1)).strftime("%Y-%m-%d")

    return {
        "run_state": run_state,
        "forced_date": forced_date,
        "start_date": start_date,
        "end_date": end_date,
        "next_date": next_date
    }
