# Virtual private environment
## Implementation
All modules and services were deployed in docker. There is executor container to execute tasks and airflow container to monitoring the pipeline, they should communicate via gRPC. One notice before running the docker compose that this version of airflow might have not permission to access /opt/airflow/logs, so I recommend you to use chmod command to give permission before mounting.
```
mkdir ../../tmp/airflow_logs
chmod -R 777 ../../tmp/airflow_logs
```

## Executor
Since the main language is Python, I chose [minconda3](https://hub.docker.com/r/continuumio/miniconda3) as base image to build the service. Due to the flexibility and isolated feature of conda that could easily to implement and run multiple Python task in that container without dependency conflicting. Each description file writen in .yml contains necessary packages for each pipeline, and each pipeline represented in a conda env. Thanks to the help of this [article](https://pythonspeed.com/articles/activate-conda-dockerfile/) :D. The Dockerfile for executor image stored in py_executor folder. 

There are 2 description file in .yml for 2 conda environments, one for ingestion pipeline and other for gRPC communication. ( in dev ) 

You might build the image in order to use for docker-compose. Just run the following command to build it locally 
```
docker image build -t py_executor:<optional tag> ./py_executor
```
or pull it directly from my docker hub
```
docker image pull j3r3my0612/py_executor:1.03
```

## RPC communication 
As mentioned, the airflow scheduler just managed the pipeline while the executor actually does the tasks. I used a simple protobuf to generate a gRPC conversation between scheduler & executor.
### Requirements

Since the gRPC python components are essential for 2 container & a message service definiton is needed, I packaged the dependencies in airflow dockerfile and executor dockerfile. It would automatically install all dependencies along with compose up.



