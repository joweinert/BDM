FROM apache/airflow:2.7.3


# Switch to root for file operations
USER root
WORKDIR /opt/airflow

# Copy own utility package and fix permissions
COPY ../../src/util /opt/airflow/util
RUN chown -R airflow:root /opt/airflow/util

#RUN apt-get update && apt-get install -y default-jdk procps

# Switch back to airflow user for pip operations and runtime
USER airflow
RUN pip install --upgrade pip setuptools
RUN pip install -e /opt/airflow/util
#RUN pip install apache-airflow-providers-apache-spark

ENV PYTHONPATH="/opt/airflow/util:${PYTHONPATH}"

# handing over to official entrypoint