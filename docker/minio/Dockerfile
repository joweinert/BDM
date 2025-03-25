FROM minio/minio:latest

# Define build-time arguments
ARG MINIO_ROOT_USER
ARG MINIO_ROOT_PASSWORD
ARG MINIO_DATA_BUCKET
ARG MINIO_SCRIPT_BUCKET

# Set them as environment variables in the image
ENV MINIO_ROOT_USER=$MINIO_ROOT_USER
ENV MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD
ENV MINIO_DATA_BUCKET=$MINIO_DATA_BUCKET
ENV MINIO_SCRIPT_BUCKET=$MINIO_SCRIPT_BUCKET

# Copy initialization scripts into the container
COPY src/batch/pipelines/ /init-scripts/
COPY images /init-scripts/images

# Install MinIO Client (`mc`) and set up MinIO storage
RUN /bin/sh -c "minio server /data --console-address ':9001' & sleep 5 && \
    /usr/bin/mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && \
    /usr/bin/mc mb myminio/$MINIO_DATA_BUCKET || true && \
    /usr/bin/mc mb myminio/$MINIO_SCRIPT_BUCKET || true && \
    /usr/bin/mc cp --recursive /init-scripts/ myminio/$MINIO_SCRIPT_BUCKET/"

# Start MinIO Server on container startup
CMD ["minio", "server", "/data", "--console-address", ":9001"]
