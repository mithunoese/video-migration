#!/bin/bash
set -e
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
BUCKET="${S3_BUCKET:-migration-staging}"
echo "Creating S3 bucket $BUCKET on $ENDPOINT..."
aws --endpoint-url="$ENDPOINT" s3api create-bucket \
  --bucket "$BUCKET" \
  --region us-east-1 2>/dev/null || echo "Bucket already exists, continuing."
echo "Done."
