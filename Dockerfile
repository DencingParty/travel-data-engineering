FROM apache/airflow:2.9.1

# 필요한 패키지 설치
RUN pip install apache-airflow-providers-apache-spark