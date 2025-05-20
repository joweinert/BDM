#!/usr/bin/env bash
set -e

# 1) Start MinIO in the background
minio server /data --console-address ":9001" &
MINIO_PID=$!

# 2) Wait for MinIO to be ready
sleep 5

# 3) Configure mc alias for local MinIO
mc alias set myminio http://localhost:9000 \
  "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# 4) Ensure buckets exist (ignore errors if they already do)
mc mb myminio/"$MINIO_DATA_BUCKET"         || true
mc mb myminio/"$MINIO_UNSTRUCTURED_BUCKET" || true
mc mb myminio/"$EXPLOITATION_ZONE_BUCKET"  || true

# 5) Create first-tier policy (read-only on exploitation & unstructureddata)
cat > /tmp/first-tier-policy.json <<EOF
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:GetObject","s3:ListBucket"],
      "Resource":[
        "arn:aws:s3:::$EXPLOITATION_ZONE_BUCKET",
        "arn:aws:s3:::$EXPLOITATION_ZONE_BUCKET/*",
        "arn:aws:s3:::$MINIO_UNSTRUCTURED_BUCKET",
        "arn:aws:s3:::$MINIO_UNSTRUCTURED_BUCKET/*"
      ]
    }
  ]
}
EOF

mc admin policy create myminio first_tier_policy /tmp/first-tier-policy.json || true

# 6) Create second-tier policy (adds read-only on deltalake)
cat > /tmp/second-tier-policy.json <<EOF
{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:GetObject","s3:ListBucket"],
      "Resource":[
        "arn:aws:s3:::$EXPLOITATION_ZONE_BUCKET",
        "arn:aws:s3:::$EXPLOITATION_ZONE_BUCKET/*",
        "arn:aws:s3:::$MINIO_UNSTRUCTURED_BUCKET",
        "arn:aws:s3:::$MINIO_UNSTRUCTURED_BUCKET/*",
        "arn:aws:s3:::$MINIO_DATA_BUCKET",
        "arn:aws:s3:::$MINIO_DATA_BUCKET/*"
      ]
    }
  ]
}
EOF

mc admin policy create myminio second_tier_policy /tmp/second-tier-policy.json || true

# 7) Add users (ignore if they already exist)
mc admin user add myminio \
   "$MINIO_FIRST_TEAR_ANALYST_USER" \
   "$MINIO_FIRST_TEAR_ANALYST_PASSWORD" || true

mc admin user add myminio \
   "$MINIO_SECOND_TEAR_ANALYST_USER" \
   "$MINIO_SECOND_TEAR_ANALYST_PASSWORD" || true

# 8) Attach policies to users
mc admin policy attach myminio first_tier_policy \
   --user "$MINIO_FIRST_TEAR_ANALYST_USER"

mc admin policy attach myminio second_tier_policy \
   --user "$MINIO_SECOND_TEAR_ANALYST_USER"

# 9) Bring MinIO process back to foreground
wait ${MINIO_PID}
