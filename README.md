# 국내 여행 로그 데이터 Pipeline & Dashboard 구축
- [데이터 사이언스 연합 동아리 EPOCH](https://sites.google.com/view/epoch-/home?authuser=2)
    - 팀 데엔싱파티 프로젝트 (2024/11/20 ~ 2024/01/24)
## 프로젝트 개요
### 배경 및 데이터 소개
- 데이터가 주기적으로 업데이트되는 환경을 가정, 과거 시점의 데이터를 현재 실행한다고 가정하고 시간의 흐름에 따라 처리
- [AI Hub 국내 여행 로그 데이터 (2022 ~ 2023)](https://www.aihub.or.kr/aihubdata/data/list.do?pageIndex=1&currMenu=115&topMenu=100&dataSetSn=&srchdataClCode=DATACL001&searchKeyword=%EA%B5%AD%EB%82%B4+%EC%97%AC%ED%96%89%EB%A1%9C%EA%B7%B8+&srchDetailCnd=DETAILCND001&srchOrder=ORDER001&srchPagePer=20)
    - 다양한 테이블을 바탕으로, 다각도의 인사이트 도출 가능 (e.g. 여행자 분석, 소비 분석 등)
    - 1년 6개월치 (2022/01 ~ 2023/05) 데이터가 미리 Data Lake에 적재되어 있다고 가정
### 목표
- 여행 로그 데이터 기반 트렌드 분석 Batch Pipeline 구축
    - Airflow를 통한 1주 단위 데이터 ETL 자동화
    - 기존 데이터 (1년 6개월) + 새로운 데이터 (6개월, 2023/06 ~ 2023/12)
- Dashboard 실시간 업데이트를 통한 트렌드 분석 및 인사이트 도출
## 프로젝트 아키텍처 & ERD
### Data Pipeline
![pipeline](https://i.imgur.com/Nd2gbsI.png)
| Architecture    | Platform                                    |
| ---------- | ---------------------------------------------- |
|Data pipeline|`Apache Airflow`|
|Containerization|`Docker`|
|Data Lake|`AWS S3`|
|Data Transform|`Apache Spark`|
|Data Warehouse|`Snowflake`|
|Visualization|`Apache Superset (Preset)`|
### ERD (Snowflake)
![erd_snowflake](https://i.imgur.com/h5EjrL8.png)
## 프로젝트 프로세스
### 데이터 준비 및 구조화
- 이미 로드된 데이터(2022 ~ 2023/5, ETL 완료 상태)와 새로운 데이터를 주기적으로 추가하는 Batch Pipeline 설계
### S3에 데이터 저장
- 통합된 데이터를 `S3`에 업로드
### 데이터 수집 및 ETL (`S3`)
- `Airflow`를 사용해 새로운 Batch 데이터를 `S3/raw-data`에서 읽어 데이터 처리 Pipeline 실행
    - `Spark`를 활용해 데이터 클렌징, 변환(필요한 필드 추출, 결측치 처리, 중복 제거 등)
    - 변환된 데이터를 `S3/processed-data`에 적재
### Data Warehouse (`Snowflake`) Schema 설계
- 시간별, 지역별, 카테고리별로 분석 가능하도록 스키마 설계
- `region_raw_data`: `S3`에서 가져온 원본 데이터 저장
- `region_ad_hoc`: 실험/테스트 데이터 위한 스키마
- `region_processed_data`: 정규화 및 가공된 데이터 저장
- `region_analysis`: 최종 분석 데이터를 저장 (시각화 목적)
### ETL 프로세스 (`Snowflake`)
#### Initial 데이터 (1년 6개월치 데이터) 
1. 데이터 필터링: 2022/01 ~ 2023/05
2. Spark 작업: `S3/processed-data`의 데이터를 Spark를 통해 클렌징 및 변환
3. Raw Data Schema 생성 및 데이터 적재
#### Weely 데이터 (새롭게 추가되는 6개월치 데이터)
1. DAG 스케줄링 및 Variable 설정: 1주별 실행 주기 (`@weekly`)
    - Airflow Variable을 통해, 과거의 특정한 시점으로 DAG 실행 날짜 고정
    - DAG가 Trigger될 때마다, DAG 실행 날짜를 1주일 후로 변환 
2. 데이터 필터링: 특정 날짜의 데이터를 필터링
3. Spark 작업
    - S3에 적재된 데이터를 Spark를 통해 데이터 클렌징 및 변환
4. Raw Data Schema 갱신
    - Snowflake의 `region_raw_data` 테이블 업데이트 (1주일 치 데이터 추가)
5. Processed Data Schema 갱신
    - `region_raw_data`의 데이터 정규화 및 가공을 통해, Snowflake `region_processed_data` 테이블 업데이트
6. Analysis Schema 업데이트
    - 새로운 데이터를 포함하도록 Snowflake `region_analysis` 테이블 업데이트
7. 다음 실행 주기를 위한 Variable 업데이트
    - e.g. 2023/06/04 → 2023/06/11
### 데이터 분석 및 시각화
- 데이터 웨어하우스에서 데이터를 조회해 주요 인사이트 도출
- `Apache Superset (Preset)` 사용
## Dashboard
![Dashboard](https://i.imgur.com/WX1MB28.jpeg)
## 작업 관리
- Jira 티켓 생성 후 작업 시작 및 GitHub 커밋
    - 칸반 보드 기반의 워크플로우 및 일정 관리
    - 티켓과 동명의 GitHub 브랜치 생성하여, 커밋 및 Pull Request 리뷰 진행
        - Jira와 GitHub 연동에 기반한 버전 단위 과업 관리
- 커밋 컨벤션 통일에 기반한 작업 및 분류 효율화
- Discord를 통한 실시간 커뮤니케이션 및 Notion, Jira 기반 미팅 내용, 작업물 아카이빙
## 회고 및 개선 사항
### 배운 점
- 데이터 엔지니어링 아키텍처 경험
    - Cloud Storage, 분산 처리, Cloud Data Warehouse, Orchestration, Containerization에 기반한 End-to-End Pipeline 구축
    - 리팩토링 및 모듈화를 통한 작업 및 코드 효율화
- 다양한 Tool을 활용한 협업 과정
    - Jira, Notion 등 목표에 툴을 활용하여, 능률적인 작업 및 일정 관리
### 아쉬운 점
- 과거 시점이라는 데이터 자체의 한계로 인해, 현재 작업 시점에서의 인사이트 도출 불가능
- 데이터 크기 및 리소스 문제로 인한, GPS 데이터 미사용 (Region 데이터에 국한)
### 개선 방안
- GPS 데이터를 추가하여, 여행자들의 이동 경로 시각화 및 주요 이동 패턴 파악
- 추가 외부 데이터를 활용한 현재 시점 (2025년)과 과거 2년간 데이터 사이의 추세 비교 등을 통해, 과거 데이터를 사용한 인사이트라는 한계 극복