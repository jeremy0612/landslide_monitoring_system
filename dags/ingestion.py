from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.grpc.operators.grpc import GrpcOperator
from airflow.contrib.sensors.file_sensor import FileSensor
# proto module
from protos.order_pb2 import ExeTask, PingRequest, PingResponse
from protos.order_pb2_grpc import PingServiceStub

from datetime import datetime
import os


def response_handler(response, context):
    print("Response received from executor")
    print("Response : " + response.message)



with DAG(
    dag_id="ingestion",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    start = DummyOperator(task_id="start")

    # order = GrpcOperator(
    #     task_id="order",
    #     # gRPC service stub class (must be imported from a generated grpc file)
    #     stub_class=PingServiceStub,
    #     # Method to call when this DAG is triggered
    #     call_func='ping',
    #     # Custom data sent to the "ping" method (can be the request, or grpc metadata)
    #     data={'request': PingResponse()},
    #     # Handler called on responses
    #     response_callback=response_handler,
    #     streaming=False,
    #     # New argument for gRPC connection details (replace with your actual connection ID)
    #     grpc_conn_id='grpc_default'
    # )


    extract_csv = GrpcOperator(
        task_id="extract_csv",
        stub_class=PingServiceStub,
        call_func='command',
        data={'request': ExeTask(command="extract_csv")},
        response_callback=response_handler,
        grpc_conn_id='grpc_default',
        retries=10,
        retry_delay=10,
        trigger_rule='all_success'
    )

    extracted_csv = FileSensor(
        task_id="extracted_csv",
        filepath='/opt/airflow/buffer/origin/date_csv.json',
        poke_interval=10,
        timeout=100
    )

    extract_xlsx = GrpcOperator(
        task_id="extract_xlsx",
        stub_class=PingServiceStub,
        call_func='command',
        data={'request': ExeTask(command="extract_xlsx")},
        response_callback=response_handler,
        grpc_conn_id='grpc_default',
        retries=10,
        retry_delay=10,
        trigger_rule='all_success'
    )

    extracted_xlsx = FileSensor(
        task_id="extracted_xlsx",
        filepath='/opt/airflow/buffer/origin/date_xlsx.json',
        poke_interval=10,
        timeout=100
    )

    crawl_csv = GrpcOperator(
        task_id="crawl_csv",
        stub_class=PingServiceStub,
        call_func='command',
        data={'request': ExeTask(command="crawl_csv")},
        response_callback=response_handler,
        grpc_conn_id='grpc_default',
        retries=10,
        retry_delay=10,
        trigger_rule='all_success'
    )

    # crawled_csv = FileSensor(
    #     task_id="crawled_csv",
    #     filepath='/opt/airflow/buffer/origin/metrics_csv.csv',
    #     poke_interval=10,
    #     timeout=100
    # )

    crawl_xlsx = GrpcOperator(
        task_id="crawl_xlsx",
        stub_class=PingServiceStub,
        call_func='command',
        data={'request': ExeTask(command="crawl_xlsx")},
        response_callback=response_handler,
        grpc_conn_id='grpc_default',
        retries=10,
        retry_delay=10,
        trigger_rule='all_success'
    )

    # crawled_xlsx = FileSensor(
    #     task_id="crawled_xlsx",
    #     filepath='/opt/airflow/buffer/origin/metrics_xlsx.csv',
    #     poke_interval=10,
    #     timeout=100
    # )

    # import_csv = GrpcOperator(
    #     task_id="import_csv",
    #     stub_class=PingServiceStub,
    #     call_func='command',
    #     data={'request': ExeTask(command="import_csv")},
    #     response_callback=response_handler,
    #     grpc_conn_id='grpc_default',
    #     retries=10,
    #     retry_delay=10,
    #     trigger_rule='all_success'
    # )

    # import_xlsx = GrpcOperator(
    #     task_id="import_xlsx",
    #     stub_class=PingServiceStub,
    #     call_func='command',
    #     data={'request': ExeTask(command="import_xlsx")},
    #     response_callback=response_handler,
    #     grpc_conn_id='grpc_default',
    #     retries=10,
    #     retry_delay=10,
    #     trigger_rule='all_success'
    # )

    end = DummyOperator(task_id="end")

    # start >> extract_csv >> extracted_csv >> crawl_csv >> crawled_csv >> import_csv >> end
    start >> [extract_csv, extract_xlsx] 
    extract_csv >> extracted_csv >> crawl_csv #>> crawled_csv >> import_csv
    extract_xlsx >> extracted_xlsx >> crawl_xlsx #>> crawled_xlsx >> import_xlsx
    [crawl_csv, crawl_xlsx] >> end 
    # [import_csv, import_xlsx] >> end