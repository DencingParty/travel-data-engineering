from datetime import datetime
from airflow.models import Variable

TEST_MODE = True  # True이면 10분 간격, False면 주간 실행
START_DATE = datetime(2023, 6, 4)
CURRENT_DATE = Variable.get("current_date", default_var="2023-06-11")  # 강제로 현재 시간을 2023년 6월 11일로 설정 (Variable 사용)
RUN_STATE_FLAG = "dag_run_state"  # 통합 Variable