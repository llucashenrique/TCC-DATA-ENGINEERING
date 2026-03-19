FROM apache/airflow:2.8.1

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    minio 

RUN pip install kaggle==1.7.4.5

USER airflow