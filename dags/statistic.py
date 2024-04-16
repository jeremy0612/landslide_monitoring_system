from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator as SparkSubmitOperator2

class SparkSubmitOperatorXCom(SparkSubmitOperator2):
    def execute(self, context):
        super().execute(context)
        return self._hook._driver_status
    
from datetime import datetime

with DAG (
    'statistic',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2021, 1, 1)
) as dag:
    spark_submit = SparkSubmitOperator(
        task_id='spark_submit',
        application='/opt/airflow/dags/spark_job/sample.py',
        conn_id='spark_default',
        jars='/opt/airflow/dags/spark_job/postgresql-42.3.9.jar'
    )

    spark_xcom = SparkSubmitOperatorXCom(
        task_id='spark_xcom',
        application='/opt/airflow/dags/spark_job/sample.py',
        conn_id='spark_default',
        jars='/opt/airflow/dags/spark_job/postgresql-42.3.9.jar',
        do_xcom_push=True
    )

    spark_model = SparkSubmitOperator(
        task_id='spark_model',
        application='/opt/airflow/dags/spark_job/sample_model.py',
        conn_id='spark_default'
    )