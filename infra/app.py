#!/usr/bin/env python3
"""
Video Migration CDK App — deploys the full Kaltura-to-Zoom migration
infrastructure.  Every resource is namespaced by a project identifier
so multiple migrations (IFRS, Indeed, …) can coexist in one account.

Usage:
    cdk deploy --all -c project=ifrs
    cdk deploy --all -c project=indeed
    cdk destroy --all -c project=ifrs
"""

import aws_cdk as cdk

from stacks.shared_infra import SharedInfraStack
from stacks.state_tracking import StateTrackingStack
from stacks.staging_bucket import StagingBucketStack
from stacks.control_plane import ControlPlaneStack
from stacks.data_plane import DataPlaneStack
from stacks.monitoring import MonitoringStack


app = cdk.App()

project = app.node.try_get_context("project") or "default"
alarm_email = app.node.try_get_context("alarm_email") or ""

env = cdk.Environment(
    account=cdk.Aws.ACCOUNT_ID,
    region=cdk.Aws.REGION,
)

prefix = f"VideoMigration-{project.upper()}"

# ── Stack 1: VPC, KMS, Secrets Manager ──────────────────────────────
shared = SharedInfraStack(
    app,
    f"{prefix}-SharedInfra",
    project_name=project,
    env=env,
)

# ── Stack 2: DynamoDB state + mapping tables ────────────────────────
state = StateTrackingStack(
    app,
    f"{prefix}-StateTracking",
    project_name=project,
    kms_key=shared.kms_key,
    env=env,
)

# ── Stack 3: S3 staging bucket ──────────────────────────────────────
staging = StagingBucketStack(
    app,
    f"{prefix}-StagingBucket",
    project_name=project,
    kms_key=shared.kms_key,
    env=env,
)

# ── Stack 4: Step Functions + Lambda control plane ──────────────────
control = ControlPlaneStack(
    app,
    f"{prefix}-ControlPlane",
    project_name=project,
    kms_key=shared.kms_key,
    vpc=shared.vpc,
    state_table=state.state_table,
    mapping_table=state.mapping_table,
    staging_bucket=staging.bucket,
    kaltura_secret=shared.kaltura_secret,
    zoom_secret=shared.zoom_secret,
    env=env,
)

# ── Stack 5: ECS Fargate + SQS data plane ──────────────────────────
data = DataPlaneStack(
    app,
    f"{prefix}-DataPlane",
    project_name=project,
    kms_key=shared.kms_key,
    vpc=shared.vpc,
    state_table=state.state_table,
    mapping_table=state.mapping_table,
    staging_bucket=staging.bucket,
    kaltura_secret=shared.kaltura_secret,
    zoom_secret=shared.zoom_secret,
    state_machine=control.state_machine,
    env=env,
)

# ── Stack 6: CloudWatch dashboards + alarms ─────────────────────────
MonitoringStack(
    app,
    f"{prefix}-Monitoring",
    project_name=project,
    state_table=state.state_table,
    staging_bucket=staging.bucket,
    job_queue=data.job_queue,
    dlq=data.dlq,
    state_machine=control.state_machine,
    ecs_cluster=data.cluster,
    fargate_service=data.fargate_service,
    lambda_functions=control.lambda_functions,
    alarm_email=alarm_email,
    env=env,
)

# Tag every resource for cost tracking and identification
for stack in app.node.children:
    if isinstance(stack, cdk.Stack):
        cdk.Tags.of(stack).add("Project", f"video-migration-{project}")
        cdk.Tags.of(stack).add("ManagedBy", "cdk")

app.synth()
