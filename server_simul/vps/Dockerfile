FROM apache/airflow:2.4.2
USER root
RUN mkdir -p "/content"
# setup pkgs
RUN apt-get update && apt-get install -y wget gnupg

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
# pip
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3
RUN pip install -U apache-airflow-providers-apache-spark pyspark findspark --user