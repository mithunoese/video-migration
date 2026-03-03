#!/usr/bin/env bash
# LocalStack initialization script for video-migration dev environment
# Usage: ./scripts/localstack-init.sh
#
# Prerequisites: Docker Desktop running
# This script starts LocalStack and creates the required AWS resources.

set -euo pipefail

BUCKET_NAME="${AWS_S3_BUCKET:-video-migration-staging}"
TABLE_NAME="${AWS_STATE_TABLE:-video-migration-state}"
REGION="${AWS_REGION:-us-east-1}"
ENDPOINT="http://localhost:4566"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "🐳 Starting LocalStack..."

# Remove existing container if present
docker rm -f localstack 2>/dev/null || true

docker run -d \
  --name localstack \
  -p 4566:4566 \
  -e SERVICES=s3,dynamodb \
  -e DEFAULT_REGION="$REGION" \
  --restart unless-stopped \
  localstack/localstack:latest

echo "⏳ Waiting for LocalStack to be ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:4566/_localstack/health > /dev/null 2>&1; then
    echo "✅ LocalStack is healthy"
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "❌ LocalStack failed to start within 30s"
    exit 1
  fi
done

echo "📦 Creating S3 bucket: $BUCKET_NAME"
aws --endpoint-url="$ENDPOINT" s3 mb "s3://$BUCKET_NAME" --region "$REGION" 2>/dev/null || echo "   (bucket already exists)"

echo "📊 Creating DynamoDB table: $TABLE_NAME"
aws --endpoint-url="$ENDPOINT" dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=video_id,AttributeType=S \
    AttributeName=status,AttributeType=S \
  --key-schema AttributeName=video_id,KeyType=HASH \
  --global-secondary-indexes \
    'IndexName=status-index,KeySchema=[{AttributeName=status,KeyType=HASH}],Projection={ProjectionType=ALL}' \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" > /dev/null 2>&1 || echo "   (table already exists)"

echo ""
echo "🎉 LocalStack ready!"
echo "   S3 bucket:      s3://$BUCKET_NAME"
echo "   DynamoDB table:  $TABLE_NAME"
echo "   Endpoint:        $ENDPOINT"
echo ""
echo "   Add to .env:"
echo "     SKIP_S3=false"
echo "     AWS_S3_BUCKET=$BUCKET_NAME"
echo "     AWS_REGION=$REGION"
echo "     AWS_STATE_TABLE=$TABLE_NAME"
echo "     AWS_ENDPOINT_URL=$ENDPOINT"
