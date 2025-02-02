import os
from jinja2 import Template
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from region_weekly_etl.config import SQL_DIR
import logging

logger = logging.getLogger(__name__)

def run_sql_task(sql_filename, task_type, sql_params):
    """
    SQL 파일을 읽어 SnowflakeHook를 통해 실행합니다.
    """
    
    sql_path = os.path.join(SQL_DIR, sql_filename)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"{sql_path} 파일을 찾을 수 없습니다.")
    
    with open(sql_path, "r") as sql_file:
        sql_template = sql_file.read()
    
    # Jinja2 템플릿 렌더링
    try:
        if sql_params:
            template = Template(sql_template)
            sql_query = template.render(sql_params)
        else:
            sql_query = sql_template
    except Exception as e:
        logger.error(f"SQL 템플릿 렌더링 중 오류 발생 ({task_type}): {str(e)}")
        raise e

    # Snowflake 연결 및 SQL 실행
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    try:
        # Snowflake 연결
        logger.info(f"SQL 쿼리 파일 실행 중 ({task_type}): {sql_filename}")
        snowflake_conn = snowflake_hook.get_conn()
        cursor = snowflake_conn.cursor()

        # SQL 문을 세미콜론으로 분리하여 개별적으로 실행
        statements = [query.strip() for query in sql_query.split(';') if query.strip()]
        for idx, statement in enumerate(statements, start=1):
            logger.info(f"실행 중인 SQL 쿼리 {idx}/{len(statements)}: {statement[:200]}...")
            cursor.execute(statement)
            logger.info(f"Statement {idx} affected {cursor.rowcount} rows.")

        logger.info(f"모든 SQL 문을 성공적으로 실행했습니다. ({task_type}): {sql_filename}")

    except Exception as e:
        # 실행 중 에러 발생 시 로그 기록 및 재전파
        logger.error(f"SQL 파일 실행 중 오류 발생 ({task_type}): {sql_filename}: {str(e)}")
        raise e

    finally:
        # 커넥션 종료
        cursor.close()
        snowflake_conn.close()
        
# Snowflake SQL 실행을 한번만 또는 스케줄에 맞게 실행하는 함수
def sql_once_or_scheduled(run_state, sql_filename_once=None, sql_filename_scheduled=None, schema="RAW_DATA", start_date=None):
    """
    SQL 파일을 읽어 SnowflakeHook를 통해 실행합니다.
    run_state에 따라 초기 작업 -> schedule 작업 순으로 실행하거나, schedule 작업만 실행합니다.
    """
    
    # 동적으로 사용할 날짜 변수
    sql_params = {"start_date": start_date} 

    if not sql_filename_scheduled:
        raise ValueError("schedule 작업을 위해 sql_filename_scheduled이 필요합니다.")

    logger.info(f"작업 중인 날짜: {start_date}")
    logger.info(f"작업 중인 Snowflake Schema: {schema}")

    # 초기 작업 (공통 처리)
    if run_state in ["pending", "force_reset"]:
        if not sql_filename_once:
           logger.info(f"{schema} sql_filename_once가 없습니다. 초기 작업을 건너뜁니다.")
        else:
            reset_mode = "reset" if schema == "RAW_DATA" else "running-once"
            logger.info(f"run_state가 {run_state}입니다. Snowflake {schema} 스키마의 초기 작업을 수행합니다. ({reset_mode})")
            run_sql_task(sql_filename_once, reset_mode, sql_params)

    else:
        logger.info(f"run_state가 {run_state} 입니다. Snowflake {schema} 스키마의 초기 작업을 건너뜁니다.")

    # schedule 작업 (공통 처리)
    schedule_mode = "scheduled"
    logger.info(f"Snowflake {schema} 스키마의 데이터를 스케쥴링하여 처리합니다. ({schedule_mode})")
    run_sql_task(sql_filename_scheduled, schedule_mode, sql_params)