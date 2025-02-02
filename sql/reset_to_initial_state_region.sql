-- activity_consume 테이블 초기화 (< 2023-06-04)
CREATE OR REPLACE TABLE travel_data.region_raw_data.activity_consume AS
    SELECT *
    FROM travel_data.region_raw_data.activity_consume
    WHERE PAYMENT_DT_YMD < '2023-06-04';

-- lodge_consume 테이블 초기화 (< 2023-06-04)
CREATE OR REPLACE TABLE travel_data.region_raw_data.lodge_consume AS
    SELECT *
    FROM travel_data.region_raw_data.lodge_consume
    WHERE PAYMENT_DT_YMD < '2023-06-04';

-- move 테이블 초기화 (< 2023-06-04)
CREATE OR REPLACE TABLE travel_data.region_raw_data.move AS
    SELECT *
    FROM travel_data.region_raw_data.move
    WHERE END_DT_YMD < '2023-06-04';

-- mvmn_consume 테이블 초기화 (< 2023-06-04)
CREATE OR REPLACE TABLE travel_data.region_raw_data.mvmn_consume AS
    SELECT *
    FROM travel_data.region_raw_data.mvmn_consume
    WHERE PAYMENT_DT_YMD < '2023-06-04';

-- travel 테이블 초기화 (< 2023-06-04)
CREATE OR REPLACE TABLE travel_data.region_raw_data.travel AS
    SELECT *
    FROM travel_data.region_raw_data.travel
    WHERE TRAVEL_END_YMD < '2023-06-04';

-- visit_area_info 테이블 초기화 (< 2023-06-04)
CREATE OR REPLACE TABLE travel_data.region_raw_data.visit_area_info AS
    SELECT *
    FROM travel_data.region_raw_data.visit_area_info
    WHERE VISIT_END_YMD < '2023-06-04';