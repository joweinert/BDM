#!/bin/sh
# Start the MinIO server in the background
minio server /data --console-address ":9001" &
sleep 5

# Configure MinIO using runtime environment variables
mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb myminio/"$MINIO_DATA_BUCKET" || true
mc mb myminio/"$MINIO_SCRIPT_BUCKET" || true
mc cp --recursive /init-scripts/ myminio/"$MINIO_SCRIPT_BUCKET"/

# Keep the container running (or switch to a command that does the actual work)
exec "$@"
