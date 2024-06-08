from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import PythonOperator  # Re-include for clarity
from airflow.decorators import dag
from spark_operator_plugin import SparkSubmitOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="testing",
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as testing:
    with TaskGroup("training") as training_group:  # Create the task group explicitly
        start = EmptyOperator(task_id="start_training")
        end = EmptyOperator(task_id="end_training")

        train = SparkSubmitOperator(
            task_id='train_test',
            application_file="/opt/airflow/dags/spark_job/model_trainer2.py",
            jars="/opt/airflow/dags/spark_job/postgresql-42.3.9.jar",
            application_args="--experiment_name landslide_detection --experiment_id 166249389057750071 --model_name detector_region_2",
            dag=testing
        )

        start >> train >> end

