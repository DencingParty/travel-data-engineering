/* s3 initial 폴더 업데이트 후 기존 데이터를 truncate하고 새로 업데이트 적용 */
TRUNCATE TABLE travel_data.region_raw_data.meta_codea;
TRUNCATE TABLE travel_data.region_raw_data.meta_codeb;
TRUNCATE TABLE travel_data.region_raw_data.meta_sgg;
TRUNCATE TABLE travel_data.region_raw_data.meta_activity;
TRUNCATE TABLE travel_data.region_raw_data.meta_traveller_master;
TRUNCATE TABLE travel_data.region_raw_data.activity_consume;
TRUNCATE TABLE travel_data.region_raw_data.lodge_consume;
TRUNCATE TABLE travel_data.region_raw_data.move;
TRUNCATE TABLE travel_data.region_raw_data.mvmn_consume;
TRUNCATE TABLE travel_data.region_raw_data.travel;
TRUNCATE TABLE travel_data.region_raw_data.visit_area_info;

-- 코드A 메타 테이블
CREATE OR REPLACE TABLE region_raw_data.meta_codea (
    idx NUMBER(38, 0),
    cd_nm TEXT,
    cd_a TEXT,
    cd_memo TEXT,
    cd_memo2 REAL,
    del_flag TEXT,
    order_num NUMBER(38, 0),
    perm_write TEXT,
    perm_edit TEXT,
    perm_delete TEXT,
    ins_dt TEXT,
    edit_dt REAL,
    REGION TEXT
);

-- 코드B 메타 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.meta_codeb (
    idx NUMBER(38, 0),
    cd_nm TEXT,
    cd_a TEXT,
    cd_b TEXT,
    cd_memo TEXT,
    cd_memo2 REAL,
    del_flag TEXT,
    order_num NUMBER(38, 0),
    ins_dt TEXT,
    edit_dt TEXT,
    REGION TEXT
);


-- 시군구코드 메타 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.meta_sgg (
    SGG_CD NUMBER(38, 0),
    SGG_CD1 NUMBER(38, 0),
    SGG_CD2 REAL,
    SGG_CD3 REAL,
    SGG_CD4 REAL,
    SIDO_NM TEXT,
    SGG_NM TEXT,
    DONG_NM TEXT,
    RI_NM TEXT
);


-- 활동내역 메타 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.meta_activity (
    ACTIVITY_TYPE_CD NUMBER(38, 0),
    ACTIVITY_DTL TEXT,
    EXPND_SE TEXT,
    TRAVEL_ID TEXT,
    VISIT_AREA_ID NUMBER(38, 0)
);

-- 여행객 마스터 메타 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.meta_traveller_master (
    TRAVELER_ID TEXT,
    RESIDENCE_SGG_CD NUMBER(38, 0),
    TRAVEL_COMPANIONS_NUM NUMBER(38, 0),
    GENDER TEXT,
    AGE_GRP NUMBER(38, 0),
    EDU_NM NUMBER(38, 0),
    JOB_NM REAL,
    TRAVEL_TERM NUMBER(38, 0),
    TRAVEL_NUM NUMBER(38, 0),
    INCOME NUMBER(38, 0),
    HOUSE_INCOME REAL,
    TRAVEL_STYL_1 NUMBER(38, 0),
    TRAVEL_STYL_2 NUMBER(38, 0),
    TRAVEL_STYL_3 NUMBER(38, 0),
    TRAVEL_STATUS_START_YMD DATE,
    TRAVEL_STATUS_END_YMD DATE
);

-- 활동소비내역 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.activity_consume (
    TRAVEL_ID TEXT,
    VISIT_AREA_ID NUMBER(38, 0),
    ACTIVITY_TYPE_CD NUMBER(38, 0),
    PAYMENT_AMT_WON REAL,
    PAYMENT_DT_YMD DATE,
    STORE_NM TEXT
);

-- 숙박소비내역 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.lodge_consume (
    TRAVEL_ID TEXT,
    LODGING_NM TEXT,
    LODGING_TYPE_CD TEXT,
    ROAD_NM_ADDR TEXT,
    PAYMENT_AMT_WON NUMBER(38, 0),
    PAYMENT_DT_YMD DATE
);

-- 이동내역 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.move (
    TRAVEL_ID TEXT,
    TRIP_ID NUMBER(38, 0),
    START_DT_YMD DATE,
    END_VISIT_AREA_ID REAL,
    END_DT_YMD DATE,
    MVMN_CD_1 REAL
);

-- 이동수단소비내역 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.mvmn_consume (
    TRAVEL_ID TEXT,
    MVMN_SE_NM TEXT,
    PAYMENT_AMT_WON REAL,
    PAYMENT_DT_YMD DATE
);

-- 여행 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.travel (
    TRAVEL_ID TEXT,
    TRAVELER_ID TEXT,
    TRAVEL_START_YMD TEXT,
    TRAVEL_END_YMD DATE,
    TRAVEL_NM TEXT
);

-- 방문지정보 테이블
CREATE OR REPLACE TABLE travel_data.region_raw_data.visit_area_info (
    TRAVEL_ID TEXT,
    VISIT_AREA_ID NUMBER(38, 0),
    VISIT_AREA_NM TEXT,
    ROAD_NM_ADDR TEXT,
    LOTNO_ADDR TEXT,
    X_COORD REAL,
    Y_COORD REAL,
    RCMDTN_INTENTION REAL,
    REVISIT_INTENTION REAL,
    DGSTFN REAL,
    VISIT_END_YMD DATE
);