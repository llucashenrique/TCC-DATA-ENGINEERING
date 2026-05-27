import os
import yaml
import importlib

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

DAGS_FOLDER = "/opt/airflow/dags"


def load_callable(path):
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def create_task(task_conf, dag):

    operator = task_conf["operator"]
    task_id = task_conf["task_id"]
    params = task_conf.get("params", {})

    if operator.endswith("EmptyOperator"):

        return EmptyOperator(task_id=task_id, dag=dag)

    if operator.endswith("PythonOperator"):

        callable_path = params["python_callable"]

        return PythonOperator(
            task_id=task_id,
            python_callable=load_callable(callable_path),
            retries=params.get("retries", 0),
            retry_delay=params.get("retry_delay"),
            dag=dag,
        )

    if operator.endswith("SparkSubmitOperator"):

        return SparkSubmitOperator(
            task_id=task_id,
            application=params["application"],
            conn_id=params["conn_id"],
            application_args=params.get("application_args", []),
            jars=params.get("jars"),
            conf=params.get("conf"),
            dag=dag,
        )


def load_dag(config_path):

    with open(config_path) as f:
        config = yaml.safe_load(f)

    dag = DAG(
        dag_id=config["dag_id"],
        description=config.get("description"),
        start_date=config["start_date"],
        schedule_interval=config.get("schedule_interval"),
        catchup=config.get("catchup", False),
        tags=config.get("tags", []),
    )

    tasks = {}

    for task_conf in config["tasks"]:

        task = create_task(task_conf, dag)

        tasks[task_conf["task_id"]] = task

    for task_conf in config["tasks"]:

        if "dependencies" in task_conf:

            for dep in task_conf["dependencies"]:

                tasks[dep] >> tasks[task_conf["task_id"]]

    return dag


for root, dirs, files in os.walk(DAGS_FOLDER):
    for file in files:
        if file.endswith(".yaml"):
            path = os.path.join(root, file)
            dag = load_dag(path)
            globals()[dag.dag_id] = dag
