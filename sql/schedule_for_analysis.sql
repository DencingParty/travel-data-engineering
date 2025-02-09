-- 1. 여행자 분석

-- 1-1. 누적 여행자 수와 누적 여행 수
-- REGION_ANALYSIS.CUMULATIVE_TRAVEL_STATS 
-- -> 누적 여행자 수 및 여행 수를 월 단위로 분석

-- 누적 여행자 수와 누적 여행 수 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.CUMULATIVE_TRAVEL_STATS;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.CUMULATIVE_TRAVEL_STATS AS
SELECT
    DATE_TRUNC('MONTH', TO_DATE(T.TRAVEL_START_YMD)) AS MONTH, -- 여행 시작 월 기준
    COUNT(DISTINCT T.TRAVELER_ID) AS UNIQUE_TRAVELERS,          -- 고유 여행자 수
    COUNT(DISTINCT T.TRAVEL_ID) AS UNIQUE_TRAVELS              -- 고유 여행 수
FROM
    TRAVEL_DATA.REGION_RAW_DATA.TRAVEL T
WHERE
    T.TRAVEL_START_YMD IS NOT NULL
GROUP BY
    DATE_TRUNC('MONTH', TO_DATE(T.TRAVEL_START_YMD))
ORDER BY
    MONTH;

-- 1-2. 성별 및 연령대 분포
-- REGION_ANALYSIS.TRAVELER_GENDER_AGE_DISTRIBUTION 
-- -> 여행자의 성별 및 연령대 비율

-- 여행자 성별 및 연령대 분포 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.TRAVELER_GENDER_AGE_DISTRIBUTION;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.TRAVELER_GENDER_AGE_DISTRIBUTION AS
SELECT
    TM.GENDER AS GENDER,                -- 성별
    TM.AGE_GRP AS AGE_GROUP,            -- 연령대
    COUNT(*) AS TRAVELER_COUNT          -- 여행자 수
FROM
    TRAVEL_DATA.REGION_RAW_DATA.META_TRAVELLER_MASTER AS TM
WHERE
    TM.GENDER IS NOT NULL AND TM.AGE_GRP IS NOT NULL -- 유효한 데이터만 포함
GROUP BY
    TM.GENDER, TM.AGE_GRP
ORDER BY
    TRAVELER_COUNT DESC;

-- 1-3. 직업별 여행 빈도
-- REGION_ANALYSIS.TRAVELER_JOB_DISTRIBUTION 
-- -> 직업군에 따라 여행 빈도를 비교

-- 직업별 여행 빈도 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.TRAVELER_JOB_DISTRIBUTION;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.TRAVELER_JOB_DISTRIBUTION AS
SELECT
    MC.CD_NM AS JOB_NAME,               -- 직업 이름
    COUNT(DISTINCT T.TRAVEL_ID) AS TRAVEL_FREQUENCY -- 여행 빈도 (고유 여행 ID 기준)
FROM
    TRAVEL_DATA.REGION_RAW_DATA.META_TRAVELLER_MASTER AS TM
INNER JOIN
    TRAVEL_DATA.REGION_RAW_DATA.TRAVEL AS T
    ON TM.TRAVELER_ID = T.TRAVELER_ID   -- 여행자와 여행 데이터 연결
INNER JOIN
    (SELECT CD_A, CD_B, CD_NM 
     FROM TRAVEL_DATA.REGION_PROCESSED_DATA.META_CODEB_DISTINCT 
     WHERE CD_A = 'JOB') AS MC          -- 코드 테이블에서 직업 이름 가져오기
    ON TM.JOB_NM = MC.CD_B              -- 직업 코드와 이름 매핑
WHERE
    TM.JOB_NM IS NOT NULL               -- 직업 정보가 유효한 데이터만 포함
GROUP BY
    MC.CD_NM
ORDER BY
    TRAVEL_FREQUENCY DESC;              -- 빈도 기준으로 내림차순 정렬

-- 2. 지역 기반 소비 분석

-- 2-1. 지역별 방문 빈도
-- REGION_ANALYSIS.REGION_VISIT_DATASET 
-- -> SIDO(시도)와 REGION(시도_시군구)를 기준으로 지역별 방문 빈도를 집계
-- -> 동일한 여행(TRAVEL_ID)이 여러 지역을 방문한 경우, 각각의 지역이 별도로 계산
-- -> 동일 여행(TRAVEL_ID)이 동일 지역(SIDO, REGION)을 여러 번 방문해도 중복되지 않도록 처리

-- 지역별 방문 빈도 (SIDO 및 REGION 중심)
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.REGION_VISIT_DATASET;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.REGION_VISIT_DATASET AS
    SELECT *
    FROM TRAVEL_DATA.REGION_PROCESSED_DATA.REGION_VISIT_DATASET;


-- 2-2. 시도별 여행 빈도 MAP
-- REGION_ANALYSIS.REGION_VISIT_MAP 
-- -> 시도별 방문 빈도를 국제 표준 코드와 함께 지도에 표시
-- -> CASE WHEN 문으로 각 시도에 해당하는 국제 표준 코드(ISO_3166_2_CODE)를 매핑

-- 기존 테이블 삭제
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.REGION_VISIT_MAP;

-- 새로운 테이블 생성
CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.REGION_VISIT_MAP AS
SELECT 
    SIDO,                                -- 시도 정보
    REGION,                              -- 시도_시군구 결합 정보
    VISIT_COUNT,                         -- 방문 빈도
    CASE 
        WHEN SIDO = '부산' THEN 'KR-26'
        WHEN SIDO = '충북' THEN 'KR-43'
        WHEN SIDO = '충남' THEN 'KR-44'
        WHEN SIDO = '대구' THEN 'KR-27'
        WHEN SIDO = '대전' THEN 'KR-30'
        WHEN SIDO = '강원' THEN 'KR-42'
        WHEN SIDO = '광주' THEN 'KR-29'
        WHEN SIDO = '경기' THEN 'KR-41'
        WHEN SIDO = '경북' THEN 'KR-47'
        WHEN SIDO = '경남' THEN 'KR-48'
        WHEN SIDO = '인천' THEN 'KR-28'
        WHEN SIDO = '제주' THEN 'KR-49'
        WHEN SIDO = '전북' THEN 'KR-45'
        WHEN SIDO = '전남' THEN 'KR-46'
        WHEN SIDO = '세종' THEN 'KR-50'
        WHEN SIDO = '서울' THEN 'KR-11'
        WHEN SIDO = '울산' THEN 'KR-31'
        ELSE NULL
    END AS ISO_3166_2_CODE              -- 국제 표준 코드
FROM 
    TRAVEL_DATA.REGION_PROCESSED_DATA.REGION_VISIT_DATASET
ORDER BY 
    VISIT_COUNT DESC;

-- 2-3. 이동수단별 이용 빈도 및 이용 금액
-- REGION_ANALYSIS.MVMN_PRICE
-- -> 이동수단별 사용 패턴과 평균 비용을 분석

-- 이동 수단별 이용빈도 및 이용 금액
DROP TABLE IF EXISTS REGION_ANALYSIS.MVMN_PRICE;

CREATE TABLE REGION_ANALYSIS.MVMN_PRICE AS
    SELECT MC.MVMN_SE_NM, MC.PAYMENT_AMT_WON, T.TRAVEL_START_YMD
    FROM REGION_RAW_DATA.MVMN_CONSUME AS MC
    INNER JOIN REGION_RAW_DATA.TRAVEL AS T
        ON MC.TRAVEL_ID = T.TRAVEL_ID;

-- 2-4. 지역별 활동유형 분포
-- REGION_ANALYSIS.REGION_ACTIVITY_DISTRIBUTION
-- -> 지역별(시도_시군구)로 활동 유형 분포를 집계하여 각 활동 유형의 빈도를 계산

-- 지역별 활동 유형 분포 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.REGION_ACTIVITY_DISTRIBUTION;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.REGION_ACTIVITY_DISTRIBUTION AS
SELECT
    V.SIDO_SGG AS REGION,          -- 시도_시군구 결합 정보
    V.SIDO AS SIDO,                -- 시도 정보
    V.SGG AS SGG,                  -- 시군구 정보
    MA.CD_NM AS ACTIVITY_TYPE,     -- 활동 유형 이름
    COUNT(*) AS ACTIVITY_COUNT     -- 활동 유형별 빈도
FROM
    TRAVEL_DATA.REGION_PROCESSED_DATA.VISIT_AREA_INFO AS V
INNER JOIN
    TRAVEL_DATA.REGION_RAW_DATA.ACTIVITY_CONSUME AS AC
    ON V.VISIT_AREA_ID = AC.VISIT_AREA_ID
    AND V.TRAVEL_ID = AC.TRAVEL_ID
INNER JOIN
    (SELECT CD_A, CD_B, CD_NM
     FROM TRAVEL_DATA.REGION_PROCESSED_DATA.META_CODEB_DISTINCT
     WHERE CD_A = 'ACT') AS MA     -- 활동 유형 코드와 이름 매핑
    ON AC.ACTIVITY_TYPE_CD = MA.CD_B
WHERE
    V.SIDO IS NOT NULL             -- 유효한 시도 정보만 포함
GROUP BY
    V.SIDO_SGG, V.SIDO, V.SGG, MA.CD_NM -- 지역 및 활동 유형별 그룹화
ORDER BY
    SIDO, ACTIVITY_COUNT DESC;     -- 시도별 정렬 및 활동 빈도 기준 정렬


-- 2-5. 지역별 활동소비금액
-- REGION_ANALYSIS.REGION_ACTIVITY_PRICE
-- -> 활동 소비 금액을 지역 단위로 집계

-- 4-2. 지역별 활동 소비 금액 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.REGION_ACTIVITY_CONSUMPTION;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.REGION_ACTIVITY_CONSUMPTION AS
SELECT
    V.SIDO_SGG AS REGION,                 -- 시도_시군구 결합 정보
    V.SIDO AS SIDO,                       -- 시도 정보
    V.SGG AS SGG,                         -- 시군구 정보
    MA.CD_NM AS ACTIVITY_TYPE,            -- 활동 유형 이름
    SUM(AC.PAYMENT_AMT_WON) AS TOTAL_CONSUMPTION, -- 활동 소비 금액 총합
    COUNT(AC.PAYMENT_AMT_WON) AS TOTAL_PAYMENT_COUNT -- 활동 소비 결제 횟수
FROM
    TRAVEL_DATA.REGION_PROCESSED_DATA.VISIT_AREA_INFO AS V
INNER JOIN
    TRAVEL_DATA.REGION_RAW_DATA.ACTIVITY_CONSUME AS AC
    ON V.VISIT_AREA_ID = AC.VISIT_AREA_ID
    AND V.TRAVEL_ID = AC.TRAVEL_ID
INNER JOIN
    (SELECT CD_A, CD_B, CD_NM 
     FROM TRAVEL_DATA.REGION_PROCESSED_DATA.META_CODEB_DISTINCT 
     WHERE CD_A = 'ACT') AS MA
    ON AC.ACTIVITY_TYPE_CD = MA.CD_B
WHERE
    V.SIDO IS NOT NULL                     -- 유효한 시도 정보만 포함
GROUP BY
    V.SIDO_SGG, V.SIDO, V.SGG, MA.CD_NM   -- 지역 및 활동 유형별로 그룹화
ORDER BY
    SIDO, TOTAL_CONSUMPTION DESC;         -- 시도별로 정렬 및 소비 금액 기준 정렬

-- 2-6. 소비 활동 지도
-- REGION_ANALYSIS.ACTIVITY_CONSUME_BY_AREA
-- -> 활동 유형별 소비 금액과 상점(STORE_NM) 정보, 지역 좌표(X_COORD, Y_COORD)를 포함

-- 소비 활동 지도
-- REGION_ANALYSIS.ACTIVITY_CONSUME_BY_AREA 테이블 이용
DROP TABLE IF EXISTS region_analysis.activity_consume_by_area;

CREATE TABLE region_analysis.activity_consume_by_area AS
SELECT 
    STORE_NM,
    CASE 
        WHEN ACTIVITY_TYPE_CD = 1 THEN '취식'
        WHEN ACTIVITY_TYPE_CD = 2 THEN '쇼핑/구매'
        WHEN ACTIVITY_TYPE_CD = 3 THEN '체험 활동/입장 및 관람'
        WHEN ACTIVITY_TYPE_CD = 4 THEN '단순 구경/산책/걷기'
        WHEN ACTIVITY_TYPE_CD = 5 THEN '휴식'
        ELSE '기타 활동'
    END AS ACTIVITY_TYPE,
    PAYMENT_AMT_WON,
    X_COORD,
    Y_COORD
FROM 
    TRAVEL_DATA.REGION_RAW_DATA.ACTIVITY_CONSUME as ac
JOIN 
    TRAVEL_DATA.REGION_PROCESSED_DATA.VISIT_AREA_INFO as vai
ON 
    ac.visit_area_id = vai.visit_area_id 
    AND ac.STORE_NM = vai.visit_area_NM;

-- 2-7. 숙박유형별 분포 및 지역별 비용
-- REGION_ANALYSIS.LODGING_TYPE_DISTRIBUTION_WITH_COST
-- -> 숙소 유형별 분포와 지역별 비용을 시각화.

-- 숙박유형별 분포와 지역별 비용 결합 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.LODGING_TYPE_DISTRIBUTION_WITH_COST;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.LODGING_TYPE_DISTRIBUTION_WITH_COST AS
SELECT
    LC.SIDO AS SIDO,                     -- 시도 정보
    LC.SGG AS SGG,                       -- 시군구 정보
    LC.SIDO_SGG AS REGION,               -- 시도_시군구 결합 정보
    MC.CD_NM AS LODGING_TYPE,            -- 숙소 유형 이름
    COUNT(*) AS LODGING_COUNT,           -- 숙소 이용 빈도
    SUM(LC.PAYMENT_AMT_WON) AS TOTAL_COST -- 숙박 비용 총합
FROM
    TRAVEL_DATA.REGION_PROCESSED_DATA.LODGE_CONSUME AS LC
INNER JOIN
    (SELECT CD_A, CD_B, CD_NM
     FROM TRAVEL_DATA.REGION_PROCESSED_DATA.META_CODEB_DISTINCT
     WHERE CD_A = 'HTY') AS MC
    ON LC.LODGING_TYPE_CD = MC.CD_B
WHERE
    LC.LODGING_TYPE_CD IS NOT NULL
    AND LC.PAYMENT_AMT_WON IS NOT NULL
GROUP BY
    LC.SIDO, LC.SGG, LC.SIDO_SGG, MC.CD_NM
ORDER BY
    TOTAL_COST DESC;

-- 2-8. 소비금액 통계 (2-3. MVMN_PRICE 활용, 2-5. region_activity_price 활용)

-- 3. 시간 기반 트렌드 분석

-- 3-1. 주별 소비 비중
-- REGION_ANALYSIS.WEEKLY_SPENDING
-- -> 주 단위로 소비 금액의 비중을 분석해 시간 기반 소비 트렌드 파악

-- 주별 소비 금액과 비중을 포함한 테이블 생성
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.WEEKLY_SPENDING;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.WEEKLY_SPENDING AS
SELECT
    WEEK_START_DATE,
    CATEGORY,
    TOTAL_SPENDING,
    TOTAL_SPENDING / SUM(TOTAL_SPENDING) OVER (PARTITION BY WEEK_START_DATE) AS SPENDING_SHARE -- 비중 계산
FROM (
    SELECT
        DATE_TRUNC('WEEK', TO_DATE(PAYMENT_DT_YMD)) AS WEEK_START_DATE,
        'MOVEMENT' AS CATEGORY,
        SUM(PAYMENT_AMT_WON) AS TOTAL_SPENDING
    FROM
        TRAVEL_DATA.REGION_RAW_DATA.MVMN_CONSUME
    WHERE
        PAYMENT_DT_YMD IS NOT NULL
    GROUP BY
        DATE_TRUNC('WEEK', TO_DATE(PAYMENT_DT_YMD))

    UNION ALL

    SELECT
        DATE_TRUNC('WEEK', TO_DATE(PAYMENT_DT_YMD)) AS WEEK_START_DATE,
        'LODGING' AS CATEGORY,
        SUM(PAYMENT_AMT_WON) AS TOTAL_SPENDING
    FROM
        TRAVEL_DATA.REGION_RAW_DATA.LODGE_CONSUME
    WHERE
        PAYMENT_DT_YMD IS NOT NULL
    GROUP BY
        DATE_TRUNC('WEEK', TO_DATE(PAYMENT_DT_YMD))

    UNION ALL

    SELECT
        DATE_TRUNC('WEEK', TO_DATE(PAYMENT_DT_YMD)) AS WEEK_START_DATE,
        'ACTIVITY' AS CATEGORY,
        SUM(PAYMENT_AMT_WON) AS TOTAL_SPENDING
    FROM
        TRAVEL_DATA.REGION_RAW_DATA.ACTIVITY_CONSUME
    WHERE
        PAYMENT_DT_YMD IS NOT NULL
    GROUP BY
        DATE_TRUNC('WEEK', TO_DATE(PAYMENT_DT_YMD))
) AS TREND
ORDER BY
    WEEK_START_DATE, CATEGORY;

-- 3-2. 시도별 여행 시작 일자
-- REGION_ANALYSIS.REGION_VISIT_COUNT
-- -> 시도별로 여행 시작 날짜와 여행 활동을 분석

DROP TABLE IF EXISTS REGION_ANALYSIS.REGION_VISIT_COUNT;
CREATE TABLE REGION_ANALYSIS.REGION_VISIT_COUNT AS
    SELECT T.TRAVEL_START_YMD, T.TRAVEL_END_YMD, V.SIDO_SGG, V.SIDO, V.SGG
    FROM REGION_RAW_DATA.TRAVEL AS T
    INNER JOIN REGION_PROCESSED_DATA.VISIT_AREA_INFO AS V
        ON T.TRAVEL_ID = V.TRAVEL_ID;

-- 4. 방문지 소비 및 만족도 분석

-- 4-1. 방문지 소비 발생 및 소비금액 집중도 (2-6. ACTIVITY_CONSUME_BY_AREA 활용)

-- 4-2. 지역별 방문지 만족도와 재방문의사 관계
-- REGION_ANALYSIS.REGION_VISITAREA_ANALYSIS
-- -> 지역별 평균 만족도(DGSTFN)와 재방문의사(REVISIT_INTENTION)를 비교하여 상관관계를 분석.

-- 지역별 평균 만족도와 재방문의사
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.REGION_VISITAREA_ANALYSIS;

CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.REGION_VISITAREA_ANALYSIS AS
SELECT
    V.SIDO AS SIDO,                  -- 시도 정보
    V.SGG AS SGG,                    -- 시군구 정보
    V.SIDO_SGG AS REGION,            -- 시도_시군구 결합 정보
    AVG(V.DGSTFN) AS AVG_SATISFACTION, -- 평균 만족도
    AVG(V.REVISIT_INTENTION) AS AVG_REVISIT_INTENTION, -- 평균 재방문의사
    COUNT(*) AS VISIT_COUNT          -- 방문 횟수
FROM
    TRAVEL_DATA.REGION_PROCESSED_DATA.VISIT_AREA_INFO V
WHERE
    V.DGSTFN IS NOT NULL AND V.REVISIT_INTENTION IS NOT NULL
GROUP BY
    V.SIDO, V.SGG, V.SIDO_SGG
ORDER BY
    VISIT_COUNT DESC;

-- 4-3. 만족도와 재방문의사가 높은 방문지 MAP
-- REGION_ANALYSIS.REGION_SPACIAL_ANALYSIS
-- -> VISIT_AREA_INFO 테이블에서 좌표와 주관적인 정보 추출하여 테이블 생성
-- -> 시각화에서는 만족도 > 4, 재방문의사 > 4 인 방문지만을 강조하여 보여줌

-- 2. 만족도와 재방문의사가 높은 방문지 MAP
-- 기존 테이블 삭제
DROP TABLE IF EXISTS TRAVEL_DATA.REGION_ANALYSIS.REGION_SPATIAL_ANALYSIS;

-- 새로운 테이블 생성
CREATE OR REPLACE TABLE TRAVEL_DATA.REGION_ANALYSIS.REGION_SPATIAL_ANALYSIS AS
SELECT
    V.X_COORD AS LONGITUDE,        -- 경도
    V.Y_COORD AS LATITUDE,         -- 위도
    V.SIDO AS SIDO,                -- 시도 정보
    V.SGG AS SGG,                  -- 시군구 정보
    V.SIDO_SGG AS REGION,          -- 시도_시군구 결합 정보
    V.REVISIT_INTENTION AS WEIGHT, -- 가중치 (재방문의향, 시각화의 밀도 기반)
    V.DGSTFN AS SATISFACTION       -- 만족도 (추가 분석 가능)
FROM
    TRAVEL_DATA.REGION_PROCESSED_DATA.VISIT_AREA_INFO V
WHERE
    V.X_COORD IS NOT NULL          -- 유효한 경도 데이터만 포함
    AND V.Y_COORD IS NOT NULL      -- 유효한 위도 데이터만 포함
    AND V.SIDO IS NOT NULL;