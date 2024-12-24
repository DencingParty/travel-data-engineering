FROM apache/airflow:2.9.1

USER root

# Java 및 Spark 설치
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && \
    apt-get clean && \
    curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
    tar -xvzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Spark 및 관련 패키지 설치
USER airflow
RUN pip install apache-airflow-providers-apache-spark boto3 pandas

# 환경 변수 설정
ENV JAVA_HOME=/opt/bitnami/java
ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH