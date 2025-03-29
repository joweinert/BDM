FROM bitnami/spark:3.3.4

WORKDIR /opt/bitnami/spark

USER root


# Install Python dependencies
RUN pip install --upgrade pip setuptools
COPY docker/spark/requirements.txt .
RUN pip install -r requirements.txt

# Copy the 'util' package with correct ownership
COPY --chown=1001:1001 ../../src/util /opt/bitnami/spark/util
RUN pip install -e /opt/bitnami/spark/util


# Ensure Python can find the package
ENV PYTHONPATH="/opt/bitnami/spark/util:${PYTHONPATH}"

# Install required Hadoop and AWS dependencies for S3A support
RUN apt-get update && apt-get install -y wget unzip
