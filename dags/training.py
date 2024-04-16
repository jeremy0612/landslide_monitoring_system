from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
# from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task_group, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.registry import (
    CreateRegisteredModelOperator,
    CreateModelVersionOperator,
    TransitionModelVersionStageOperator,
)
from datetime import datetime
    
## MLFlow parameters
MLFLOW_CONN_ID = "mlflow_default"
EXPERIMENT_NAME = "landslide_detection"
MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS = 1000

# with DAG(
#     'training',
#     schedule_interval='@daily',
#     catchup=False,
#     start_date=datetime(2021, 1, 1)
# ) as dag:
@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False
)
def training():
    start = EmptyOperator(task_id="start_training")
    end = EmptyOperator(task_id="end_training")

    @task_group
    def prepare_mlflow_experiment():
        @task
        def list_existing_experiments(max_results=1000):
            "Get information about existing MLFlow experiments."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            existing_experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": max_results},
            ).json()

            return existing_experiments_information

        @task.branch
        def check_if_experiment_exists(
            experiment_name, existing_experiments_information
        ):
            "Check if the specified experiment already exists."

            if existing_experiments_information:
                existing_experiment_names = [
                    experiment["name"]
                    for experiment in existing_experiments_information["experiments"]
                ]
                if experiment_name in existing_experiment_names:
                    return "prepare_mlflow_experiment.experiment_exists"
                else:
                    return "prepare_mlflow_experiment.create_experiment"
            else:
                return "prepare_mlflow_experiment.create_experiment"

        @task
        def create_experiment(experiment_name):
            """Create a new MLFlow experiment with a specified name.
            Save artifacts to the specified S3 bucket."""

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            new_experiment_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/create",
                request_params={
                    "name": experiment_name,
                    "artifact_location": "/opt/airflow/dags/spark_job/mlartifacts",
                },
            ).json()

            return new_experiment_information

        experiment_already_exists = EmptyOperator(task_id="experiment_exists")

        @task(
            trigger_rule="none_failed",
        )
        def get_current_experiment_id(experiment_name, max_results=1000):
            "Get the ID of the specified MLFlow experiment."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
            experiments_information = mlflow_hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": max_results},
            ).json()

            for experiment in experiments_information["experiments"]:
                if experiment["name"] == experiment_name:
                    return experiment["experiment_id"]

            raise ValueError(f"{experiment_name} not found in MLFlow experiments.")

        experiment_id = get_current_experiment_id(
            experiment_name=EXPERIMENT_NAME,
            max_results=MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS,
        )

        (
            check_if_experiment_exists(
                experiment_name=EXPERIMENT_NAME,
                existing_experiments_information=list_existing_experiments(
                    max_results=MAX_RESULTS_MLFLOW_LIST_EXPERIMENTS
                ),
            )
            >> [
                experiment_already_exists,
                create_experiment(
                    experiment_name=EXPERIMENT_NAME
                ),
            ]
            >> experiment_id
        )
    train = SparkSubmitOperator(
        task_id='trainer_region_1',
        application='/opt/airflow/dags/spark_job/model_trainer.py',
        conn_id='spark_default',
        jars='/opt/airflow/dags/spark_job/postgresql-42.3.9.jar',
        application_args=[
            "--experiment_name", EXPERIMENT_NAME,
            "--experiment_id", "{{ task_instance.xcom_pull(task_ids='prepare_mlflow_experiment.get_current_experiment_id', key='return_value') }}",
            "--model_name", "detector_region_1",
        ]
    )
    check_current_run_id = FileSensor(
        task_id="check_current_run_id",
        filepath='/opt/airflow/buffer/mlflow/detector_region_1.txt',
        poke_interval=10,
        timeout=100
    )
    @task
    def get_run_id():
        with open('/opt/airflow/buffer/mlflow/detector_region_1.txt', 'r') as f:
            run_id = f.read()
        return run_id
    
    @task_group
    def register_model():
        @task.branch
        def check_if_model_already_registered(reg_model_name):
            "Get information about existing registered MLFlow models."

            mlflow_hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID, method="GET")
            get_reg_model_response = mlflow_hook.run(
                endpoint="api/2.0/mlflow/registered-models/get",
                request_params={"name": reg_model_name},
            ).json()

            if "error_code" in get_reg_model_response:
                if get_reg_model_response["error_code"] == "RESOURCE_DOES_NOT_EXIST":
                    reg_model_exists = False
                else:
                    raise ValueError(
                        f"Error when checking if model is registered: {get_reg_model_response['error_code']}"
                    )
            else:
                reg_model_exists = True

            if reg_model_exists:
                return "register_model.model_already_registered"
            else:
                return "register_model.create_registered_model"

        model_already_registered = EmptyOperator(task_id="model_already_registered")

        create_registered_model = CreateRegisteredModelOperator(
            task_id="create_registered_model",
            name="detector_region_1",
            tags=[
                {"key": "model_type", "value": "regression"},
                {"key": "data", "value": "landslide"},
            ],
        )

        create_model_version = CreateModelVersionOperator(
            task_id="create_model_version",
            name="detector_region_1",
            source="/opt/airflow/dags/spark_job/mlartifacts/{{ ti.xcom_pull(task_ids='get_run_id') }}/artifacts/model",
            run_id="{{ ti.xcom_pull(task_ids='get_run_id') }}",
            trigger_rule="none_failed",
        )

        transition_model = TransitionModelVersionStageOperator(
            task_id="transition_model",
            name="detector_region_1",
            version="{{ ti.xcom_pull(task_ids='register_model.create_model_version')['model_version']['version'] }}",
            stage="Staging",
            archive_existing_versions=True,
        )

        (
            check_if_model_already_registered(reg_model_name="detector_region_1")
            >> [model_already_registered, create_registered_model]
            >> create_model_version
            >> transition_model
        )


    start >> prepare_mlflow_experiment() >> train >> check_current_run_id >> get_run_id() >> register_model() >> end

training()