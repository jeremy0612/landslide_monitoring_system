from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.grpc.operators.grpc import GrpcOperator
from airflow.decorators import dag, task_group, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.pyfunc import (
    ModelLoadAndPredictOperator
)
# proto module
from protos.order_pb2 import ExeTask, PingRequest, PingResponse
from protos.order_pb2_grpc import PingServiceStub
from datetime import datetime
import pandas as pd

@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False
)
def predicting():
    start = EmptyOperator(task_id="start_predicting")
    end = EmptyOperator(task_id="end_predicting",trigger_rule='all_done')

    outline_exist = FileSensor(
        task_id="check_outline_exist",
        poke_interval=10,
        timeout=180,
        filepath="/opt/airflow/buffer/message_broker/outline.json"
    )
    prepare_data = GrpcOperator(
        task_id="prepare_data",
        stub_class=PingServiceStub,
        call_func='command',
        data={'request': ExeTask(command="crawl_predict")},
        grpc_conn_id='grpc_default',
        retries=10,
        retry_delay=10,
        trigger_rule='all_success'
    )
    data_exist = FileSensor(
        task_id="check_data_exist",
        poke_interval=10,
        timeout=180,
        filepath="/opt/airflow/buffer/message_broker/outline_data.csv"
    )

    @task
    def divide_by_region():
        df = pd.read_csv('/opt/airflow/buffer/message_broker/outline_data.csv')
        # Filter data in Northern Asia
        df_filtered1 = df[(df['longitude_info'] >= 70) & (df['longitude_info'] <= 160) & 
                        (df['latitude_info'] >= -13) & (df['latitude_info'] <= 25)]
        # Filter data in America
        df_filtered2 = df[(df['longitude_info'] >= -145) & (df['longitude_info'] <= -36) & 
                        (df['latitude_info'] >= -55) & (df['latitude_info'] <= 63)]
        # Save filtered DataFrames to separate CSV files
        if not df_filtered1.empty:
            df_filtered1.to_csv('/opt/airflow/buffer/message_broker/outline_northern_asia.csv', index=False)
        if not df_filtered2.empty:
            df_filtered2.to_csv('/opt/airflow/buffer/message_broker/outline_america.csv', index=False)
    
    @task_group(group_id="predict")
    def predict():
        america_data_exist = FileSensor(
            task_id="check_america_data_exist",
            poke_interval=10,
            timeout=180,
            filepath="/opt/airflow/buffer/message_broker/outline_america.csv"
        )
        predict_america = SparkSubmitOperator(
            task_id="predict_america_outline",
            application="/opt/airflow/dags/spark_job/model_detector.py",
            conn_id='spark_default',
            jars='/opt/airflow/dags/spark_job/postgresql-42.3.9.jar',
            application_args=[
                "--region","america",
                "--artifact_path","{{ ti.xcom_pull(dag_id='training', task_ids='register_model_region_1.create_model_version',include_prior_dates=True)['model_version']['source'] }}"
            ],
            trigger_rule='all_success'
        )
        northern_asia_data_exist = FileSensor(
            task_id="check_northern_asia_data_exist",
            poke_interval=10,
            timeout=180,
            filepath="/opt/airflow/buffer/message_broker/outline_northern_asia.csv"
        )
        predict_northern_asia = SparkSubmitOperator(
            task_id="predict_northern_asia_outline",
            application="/opt/airflow/dags/spark_job/model_detector.py",
            conn_id='spark_default',
            jars='/opt/airflow/dags/spark_job/postgresql-42.3.9.jar',
            application_args=[
                "--region","northern_asia",
                "--artifact_path","{{ ti.xcom_pull(dag_id='training', task_ids='register_model_region_2.create_model_version',include_prior_dates=True)['model_version']['source'] }}"
            ],
            trigger_rule='all_success'
        )

        america_data_exist >> predict_america
        northern_asia_data_exist >> predict_northern_asia
        

    start >> outline_exist >> prepare_data >> data_exist 
    data_exist >> divide_by_region() >> predict() >> end

airflow_predict = predicting()