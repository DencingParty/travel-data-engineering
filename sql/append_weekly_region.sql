-- activity_consume 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.activity_consume
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*region_data/tn_activity_consume_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- lodge_consume 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.lodge_consume
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*region_data/tn_lodge_consume_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- move 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.move
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*region_data/tn_move_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- mvmn_consume 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.mvmn_consume
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*region_data/tn_mvmn_consume_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- travel 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.travel
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*region_data/tn_travel_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- visit_area_info 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.visit_area_info
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*region_data/tn_visit_area_info_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;