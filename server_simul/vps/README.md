# Virtual private environment
## Implementation
All modules and services were deployed in docker. There is executor container to execute tasks and airflow container to monitoring the pipeline, they should communicate via gRPC.( future update )

Since the main language is Python, I chose [minconda3](https://hub.docker.com/r/continuumio/miniconda3) as base image to build the service. Due to the flexibility and isolated feature of conda that could easily to implement and run multiple Python task in that container without dependency conflicting. Each description file writen in .yml contains necessary packages for each pipeline, and each pipeline represented in a conda env. 