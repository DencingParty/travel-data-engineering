# Snowflake 관련 설정
SQL_DIR = "/opt/airflow/sql"

# SNOWFLAKE_TASK_CONFIGS = [
#     {"schema": "raw_data", "sql_filename_once": "reset_to_initial_state_region.sql", "sql_filename_scheduled": "schedule_append_region.sql"},
#     {"schema": "processed_data", "sql_filename_once": "once_for_processed_data.sql", "sql_filename_scheduled": "schedule_for_processed_data.sql"},
#     {"schema": "analysis", "sql_filename_once": None, "sql_filename_scheduled": "schedule_for_analysis.sql"},
# ]