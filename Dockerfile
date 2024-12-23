FROM apache/airflow:2.9.1

# USER root

# USER airflow

# 필요한 패키지 설치
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3
