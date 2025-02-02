-- meta_codea 테이블에 데이터 적재
COPY INTO travel_data.region_raw_data.meta_codea
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*metadata/tc_codea_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- meta_codeb 테이블에 데이터 적재
COPY INTO travel_data.region_raw_data.meta_codeb
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*metadata/tc_codeb_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- meta_sgg 테이블에 데이터 적재
COPY INTO travel_data.region_raw_data.meta_sgg
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*metadata/tc_sgg_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- meta_activity 테이블에 데이터 적재
COPY INTO travel_data.region_raw_data.meta_activity
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*metadata/tn_activity_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- meta_traveller_master 테이블에 데이터 적재
COPY INTO travel_data.region_raw_data.meta_traveller_master
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '.*metadata/tn_traveller_master_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

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