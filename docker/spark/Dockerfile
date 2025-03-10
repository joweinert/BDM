FROM bitnami/spark:3.3.4

WORKDIR /opt/bitnami/spark

USER root

# Copy the 'util' package with correct ownership
COPY --chown=1001:1001 ../../src/util /opt/bitnami/spark/util

# Install Python dependencies
RUN pip install --upgrade pip setuptools
RUN pip install -e /opt/bitnami/spark/util
COPY docker/spark/requirements.txt .
RUN pip install -r requirements.txt


# Ensure Python can find the package
ENV PYTHONPATH="/opt/bitnami/spark/util:${PYTHONPATH}"

# Install required Hadoop and AWS dependencies for S3A support
RUN apt-get update && apt-get install -y wget unzip

# Install Hadoop AWS and AWS Java SDK jars
#UN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P /opt/bitnami/spark/jars/ \
#    && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.375/aws-java-sdk-1.11.375.jar -P /opt/bitnami/spark/jars/

# Set up Spark configurations for MinIO access
#COPY --chown=1001:1001 ./docker/spark/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

