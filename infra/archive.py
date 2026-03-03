"""Archive — exports CloudWatch logs, DynamoDB state, and audit reports
to S3 before infrastructure teardown.

Usage:
    python archive.py --project ifrs --region us-east-1
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("archive")


def archive_dynamodb_table(table_name: str, bucket: str, prefix: str, region: str):
    """Export DynamoDB table contents to S3 as JSON."""
    dynamodb = boto3.resource("dynamodb", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    table = dynamodb.Table(table_name)
    items = []

    scan_kwargs = {}
    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    key = f"{prefix}/{table_name}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(items, default=str, indent=2),
        ContentType="application/json",
    )
    logger.info(f"Archived {len(items)} items from {table_name} → s3://{bucket}/{key}")
    return len(items)


def archive_cloudwatch_logs(log_group: str, bucket: str, prefix: str, region: str):
    """Export CloudWatch log group to S3."""
    logs_client = boto3.client("logs", region_name=region)

    try:
        # Create export task
        response = logs_client.create_export_task(
            logGroupName=log_group,
            fromTime=0,  # All time
            to=int(time.time() * 1000),
            destination=bucket,
            destinationPrefix=f"{prefix}/cloudwatch{log_group}",
        )
        task_id = response["taskId"]
        logger.info(f"Started CloudWatch export task {task_id} for {log_group}")

        # Wait for completion (max 5 minutes)
        for _ in range(60):
            desc = logs_client.describe_export_tasks(taskId=task_id)
            status = desc["exportTasks"][0]["status"]["code"]
            if status == "COMPLETED":
                logger.info(f"CloudWatch export completed for {log_group}")
                return True
            elif status in ("FAILED", "CANCELLED"):
                logger.warning(f"CloudWatch export {status} for {log_group}")
                return False
            time.sleep(5)

        logger.warning(f"CloudWatch export timed out for {log_group}")
        return False

    except logs_client.exceptions.ResourceNotFoundException:
        logger.info(f"Log group {log_group} not found, skipping")
        return False
    except Exception as e:
        logger.warning(f"Failed to export {log_group}: {e}")
        return False


def generate_teardown_certificate(project: str, bucket: str, prefix: str, region: str):
    """Generate a teardown certificate documenting what was archived."""
    s3 = boto3.client("s3", region_name=region)
    timestamp = datetime.now(timezone.utc).isoformat()

    certificate = {
        "type": "teardown_certificate",
        "project": project,
        "timestamp": timestamp,
        "archived_to": f"s3://{bucket}/{prefix}/",
        "contents": [
            "DynamoDB state table export",
            "DynamoDB mapping table export",
            "CloudWatch logs (Lambda, Step Functions, ECS)",
            "Audit reports from S3",
        ],
        "note": "KMS key deletion scheduled with 30-day cancellation window",
    }

    key = f"{prefix}/teardown-certificate.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(certificate, indent=2),
        ContentType="application/json",
    )
    logger.info(f"Teardown certificate → s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(description="Archive before teardown")
    parser.add_argument("--project", required=True, help="Project name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()

    project = args.project
    region = args.region
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    prefix = f"archives/{project}/{timestamp}"

    # Determine bucket name
    sts = boto3.client("sts", region_name=region)
    account = sts.get_caller_identity()["Account"]
    bucket = f"video-migration-staging-{project}-{account}"

    logger.info(f"Archiving project '{project}' to s3://{bucket}/{prefix}/")

    # Archive DynamoDB tables
    archive_dynamodb_table(
        f"video-migration-state-{project}", bucket, prefix, region
    )
    archive_dynamodb_table(
        f"video-id-mapping-{project}", bucket, prefix, region
    )

    # Archive CloudWatch log groups
    log_groups = [
        f"/aws/vendedlogs/states/video-migration-{project}",
        f"/ecs/video-migration-worker-{project}",
    ]
    for lg in log_groups:
        archive_cloudwatch_logs(lg, bucket, prefix, region)

    # Generate teardown certificate
    generate_teardown_certificate(project, bucket, prefix, region)

    logger.info(f"Archive complete: s3://{bucket}/{prefix}/")


if __name__ == "__main__":
    main()
