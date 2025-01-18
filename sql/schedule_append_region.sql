/* 1. REGION_RAW_DATA의 테이블 변동 사항 저장할 STREAM 생성 */

-- 세션에서 사용할 스키마 명시적으로 설정
USE SCHEMA REGION_RAW_DATA;

-- activity_consume 테이블 변동 사항 저장할 STREAM 생성
CREATE OR REPLACE STREAM activity_consume_stream_processed_data
    ON TABLE REGION_RAW_DATA.activity_consume;

-- lodge_consume 테이블 변동 사항 저장할 STREAM 생성
CREATE OR REPLACE STREAM lodge_consume_stream_processed_data
    ON TABLE REGION_RAW_DATA.lodge_consume;

-- move 테이블 변동 사항 저장할 STREAM 생성
CREATE OR REPLACE STREAM move_stream_processed_data
    ON TABLE REGION_RAW_DATA.move;

-- mvmn_consume 테이블 변동 사항 저장할 STREAM 생성
CREATE OR REPLACE STREAM mvmn_consume_stream_processed_data
    ON TABLE REGION_RAW_DATA.mvmn_consume;

-- travel 테이블 변동 사항 저장할 STREAM 생성
CREATE OR REPLACE STREAM travel_stream_processed_data
    ON TABLE REGION_RAW_DATA.travel;

-- VISIT_AREA_INFO 테이블 변동 사항 저장할 STREAM 생성
CREATE OR REPLACE STREAM visit_area_info_stream_processed_data
    ON TABLE REGION_RAW_DATA.VISIT_AREA_INFO;


/* 2. REGION_RAW_DATA에 주별 데이터 병합 */
-- activity_consume 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.activity_consume
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '{{ start_date }}/region_data/tn_activity_consume_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- lodge_consume 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.lodge_consume
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '{{ start_date }}/region_data/tn_lodge_consume_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- move 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.move
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '{{ start_date }}/region_data/tn_move_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- mvmn_consume 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.mvmn_consume
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '{{ start_date }}/region_data/tn_mvmn_consume_his_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- travel 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.travel
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '{{ start_date }}/region_data/tn_travel_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

-- visit_area_info 테이블에 주별 데이터 병합
COPY INTO travel_data.region_raw_data.visit_area_info
FROM @travel_data.region_raw_data.s3_stage
PATTERN = '{{ start_date }}/region_data/tn_visit_area_info_.*_snappy\.parquet$'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;

