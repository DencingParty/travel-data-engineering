from airflow.decorators import task
from airflow.models import Variable
import logging

from region_weekly_etl.utils.variable_helpers import get_task_context
from region_weekly_etl.config import RUN_STATE_FLAG

logger = logging.getLogger(__name__)

# get_task_context()에서 가져오는 변수를 통해 날짜 정보 가져오기
context = get_task_context()

@task
def update_variables():
    """
    Variable 자동 업데이트 태스크
    """
    next_date = context["next_date"]
    Variable.set(RUN_STATE_FLAG, "done")
    Variable.set("current_date", next_date)
    logger.info(f"Variable 'current_date'가 {next_date}(으)로 업데이트 되었습니다.")