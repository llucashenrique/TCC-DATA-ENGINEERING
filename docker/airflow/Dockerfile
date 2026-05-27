FROM apache/airflow:2.8.1

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark==3.5.1 \
    minio \
    kaggle==1.7.4.5

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH