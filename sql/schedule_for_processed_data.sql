/* 1. 스케쥴된 기간마다 테이블의 변경 사항을 추적하는 STREAM 데이터를 읽어와서 PROCESSED_DATA 테이블로 복사 */

-- 세션에서 사용할 스키마 명시적으로 설정
USE SCHEMA REGION_RAW_DATA;

-- 새롭게 SIDO, SGG, SIDO_SGG 컬럼을 추가한 STREAM만 VIEW로 변경
-- lodge_consume_stream_processed_data을 VIEW로 변경
CREATE OR REPLACE VIEW lodge_consume_stream_view AS
    SELECT TRAVEL_ID, LODGING_NM, LODGING_TYPE_CD, ROAD_NM_ADDR, PAYMENT_AMT_WON, PAYMENT_DT_YMD,
        NULL AS SIDO,
        NULL AS SGG,
        NULL AS SIDO_SGG
    FROM lodge_consume_stream_processed_data
    WHERE METADATA$ACTION = 'INSERT'; -- 새로 추가된 데이터만 선택

-- visit_area_info_stream_processed_data을 VIEW로 변경
CREATE OR REPLACE VIEW visit_area_info_stream_view AS
    SELECT TRAVEL_ID, VISIT_AREA_ID, VISIT_AREA_NM, ROAD_NM_ADDR, LOTNO_ADDR,
        X_COORD, Y_COORD, RCMDTN_INTENTION, REVISIT_INTENTION, DGSTFN, VISIT_END_YMD,
        NULL AS SIDO,
        NULL AS SGG,
        NULL AS SIDO_SGG
    FROM visit_area_info_stream_processed_data
    WHERE METADATA$ACTION = 'INSERT'; -- 새로 추가된 데이터만 선택
    

-- REGION_PROCESSED_DATA 스키마의 테이블에 변경 사항 적재

-- 새로 추가된 데이터만 lodge_stream_view에서 가져와서 REGION_PROCESSED_DATA.lodge_consume에 적재
INSERT INTO REGION_PROCESSED_DATA.lodge_consume
    SELECT TRAVEL_ID, LODGING_NM, LODGING_TYPE_CD, ROAD_NM_ADDR, PAYMENT_AMT_WON, PAYMENT_DT_YMD,
        SIDO, SGG, SIDO_SGG
    FROM lodge_consume_stream_view;

-- 새로 추가된 데이터만 visit_area_info_stream_view에서 가져와서 REGION_PROCESSED_DATA.visit_area_info에 적재
INSERT INTO REGION_PROCESSED_DATA.visit_area_info
    SELECT TRAVEL_ID, VISIT_AREA_ID, VISIT_AREA_NM, ROAD_NM_ADDR, LOTNO_ADDR,
        X_COORD, Y_COORD, RCMDTN_INTENTION, REVISIT_INTENTION, DGSTFN, VISIT_END_YMD,
        SIDO, SGG, SIDO_SGG
    FROM visit_area_info_stream_view;


/* 2. 지역 정보 파싱 & 데이터 정규화 */

-- Step 1: SIDO, SGG 값 업데이트
UPDATE REGION_PROCESSED_DATA.LODGE_CONSUME
SET 
    SIDO = CASE
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('서울특별시', '서울시', '서울') THEN '서울'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('제주특별자치도', '제주도', '제주') THEN '제주'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('경기도', '경기') THEN '경기'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('강원특별자치도', '강원도', '강원') THEN '강원'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('충청남도', '충남') THEN '충남'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('충청북도', '충북') THEN '충북'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('전라남도', '전남') THEN '전남'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('전라북도', '전북') THEN '전북'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('경상남도', '경남') THEN '경남'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('경상북도', '경북') THEN '경북'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('광주광역시', '광주시', '광주') THEN '광주'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('대구광역시', '대구시', '대구') THEN '대구'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('대전광역시', '대전시', '대전') THEN '대전'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('부산광역시', '부산시', '부산') THEN '부산'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('인천광역시', '인천시', '인천') THEN '인천'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('울산광역시', '울산시', '울산') THEN '울산'
        WHEN SPLIT_PART(ROAD_NM_ADDR, ' ', 1) IN ('세종특별자치시', '세종시', '세종') THEN '세종'
        ELSE SPLIT_PART(ROAD_NM_ADDR, ' ', 1)
    END,
    SGG = SPLIT_PART(ROAD_NM_ADDR, ' ', 2);

-- Step 2: SIDO_SGG 생성
UPDATE REGION_PROCESSED_DATA.LODGE_CONSUME
SET 
    SIDO_SGG = TRIM(CONCAT(SIDO, ' ', SGG));

-- 세종시의 SGG 값을 일원화하고, SIDO_SGG 업데이트
UPDATE REGION_PROCESSED_DATA.LODGE_CONSUME
SET 
    SGG = '세종시',
    SIDO_SGG = CONCAT(SIDO, ' 세종시')
WHERE SIDO = '세종';

-- 이상 데이터 삭제
DELETE FROM REGION_PROCESSED_DATA.LODGE_CONSUME
WHERE SIDO IS NULL 
   OR SIDO = ''
   OR SIDO NOT IN (
       '서울', '제주', '경기', '강원', '충남', '충북',
       '전남', '전북', '경남', '경북', '광주', '대구',
       '대전', '부산', '인천', '울산', '세종'
   )
   OR SGG IS NULL 
   OR SGG = ''
   OR SGG NOT LIKE '%구' 
   AND SGG NOT LIKE '%시'
   AND SGG NOT LIKE '%군';

/* 2. 방문지정보 (visit_area_info) 
   2-1. LOTNO_ADDR 기반 SIDO, SGG, SIDO_SGG 값 업데이트 */
UPDATE REGION_PROCESSED_DATA.VISIT_AREA_INFO
SET 
    SIDO = CASE
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('서울특별시', '서울시', '서울') THEN '서울'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('제주특별자치도', '제주도', '제주') THEN '제주'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('경기도', '경기') THEN '경기'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('강원특별자치도', '강원도', '강원') THEN '강원'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('충청남도', '충남') THEN '충남'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('충청북도', '충북') THEN '충북'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('전라남도', '전남') THEN '전남'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('전라북도', '전북') THEN '전북'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('경상남도', '경남') THEN '경남'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('경상북도', '경북') THEN '경북'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('광주광역시', '광주시', '광주') THEN '광주'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('대구광역시', '대구시', '대구') THEN '대구'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('대전광역시', '대전시', '대전') THEN '대전'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('부산광역시', '부산시', '부산') THEN '부산'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('인천광역시', '인천시', '인천') THEN '인천'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('울산광역시', '울산시', '울산') THEN '울산'
        WHEN SPLIT_PART(LOTNO_ADDR, ' ', 1) IN ('세종특별자치시', '세종시', '세종') THEN '세종'
        ELSE SPLIT_PART(LOTNO_ADDR, ' ', 1)
    END,
    SGG = SPLIT_PART(LOTNO_ADDR, ' ', 2);

-- Step 2: SIDO_SGG 생성
UPDATE REGION_PROCESSED_DATA.VISIT_AREA_INFO
SET 
    SIDO_SGG = TRIM(CONCAT(SIDO, ' ', SGG));

-- 2-2. 세종특별자치시의 SGG 값 일원화
UPDATE REGION_PROCESSED_DATA.VISIT_AREA_INFO
SET 
    SGG = '세종시',
    SIDO_SGG = CONCAT(SIDO, ' 세종시')
WHERE SIDO = '세종';

-- 2-3. 결측치 및 이상치 제거
DELETE FROM REGION_PROCESSED_DATA.VISIT_AREA_INFO
WHERE SIDO IS NULL 
   OR SIDO = ''
   OR SIDO NOT IN (
       '서울', '제주', '경기', '강원', '충남', '충북',
       '전남', '전북', '경남', '경북', '광주', '대구',
       '대전', '부산', '인천', '울산', '세종'
   )
   OR SGG IS NULL 
   OR SGG = ''
   OR SGG NOT LIKE '%구' 
   AND SGG NOT LIKE '%시'
   AND SGG NOT LIKE '%군';


-- 3. REGION_VISIT_DATASET 테이블 업데이트

-- 지역별 방문 빈도 (SIDO 및 REGION 중심)
DROP TABLE IF EXISTS REGION_PROCESSED_DATA.REGION_VISIT_DATASET;
CREATE TABLE REGION_PROCESSED_DATA.REGION_VISIT_DATASET AS
SELECT
    REGION.SIDO AS SIDO,          -- 시도 정보
    REGION.REGION AS REGION,      -- 시도_시군구 결합 정보
    COUNT(*) AS VISIT_COUNT       -- 지역별 방문 빈도
FROM (
    -- 여행-지역 고유 조합 추출
    SELECT DISTINCT
        t.TRAVEL_ID,              -- 여행 ID
        v.SIDO,                  -- 시도 정보
        v.SIDO_SGG AS REGION     -- 시도_시군구 결합 정보
    FROM
        REGION_RAW_DATA.TRAVEL t
    INNER JOIN
        REGION_PROCESSED_DATA.VISIT_AREA_INFO v
        ON t.TRAVEL_ID = v.TRAVEL_ID
    WHERE
        v.SIDO IS NOT NULL       -- 유효한 시도 정보만 포함
) AS REGION
GROUP BY
    REGION.SIDO, 
    REGION.REGION
ORDER BY
    VISIT_COUNT DESC;
