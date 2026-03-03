"""Data Plane Stack — ECS Fargate workers + SQS job queue.

Fargate workers long-poll the SQS queue, download videos from Kaltura,
stage them in S3, upload to Zoom, and call back to Step Functions via
SendTaskSuccess / SendTaskFailure.

Auto-scales from 0 to 10 tasks based on SQS queue depth.
"""

import os

import aws_cdk as cdk
from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_iam as iam,
    aws_kms as kms,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_applicationautoscaling as appscaling,
)
from constructs import Construct


WORKER_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "worker",
)


class DataPlaneStack(cdk.Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        kms_key: kms.IKey,
        vpc: ec2.IVpc,
        state_table: dynamodb.ITable,
        mapping_table: dynamodb.ITable,
        staging_bucket: s3.IBucket,
        kaltura_secret: secretsmanager.ISecret,
        zoom_secret: secretsmanager.ISecret,
        state_machine: sfn.IStateMachine,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── SQS Job Queue ───────────────────────────────────────────
        self.dlq = sqs.Queue(
            self,
            "DLQ",
            queue_name=f"video-migration-dlq-{project_name}",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=kms_key,
            retention_period=cdk.Duration.days(14),
        )

        self.job_queue = sqs.Queue(
            self,
            "JobQueue",
            queue_name=f"video-migration-jobs-{project_name}",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=kms_key,
            visibility_timeout=cdk.Duration.seconds(1800),  # 30 min
            retention_period=cdk.Duration.days(4),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.dlq,
            ),
        )

        # ── ECS Cluster ─────────────────────────────────────────────
        self.cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name=f"video-migration-{project_name}",
            vpc=vpc,
            container_insights_v2=ecs.ContainerInsights.ENABLED,
        )

        # ── Fargate Task Definition ─────────────────────────────────
        task_role = iam.Role(
            self,
            "TaskRole",
            role_name=f"video-migration-worker-{project_name}",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        # Grant permissions to the task role
        self.job_queue.grant_consume_messages(task_role)
        state_table.grant_read_write_data(task_role)
        mapping_table.grant_read_write_data(task_role)
        staging_bucket.grant_read_write(task_role)
        kaltura_secret.grant_read(task_role)
        zoom_secret.grant_read(task_role)
        kms_key.grant_encrypt_decrypt(task_role)

        # Step Functions callback permissions
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                    "states:SendTaskHeartbeat",
                ],
                resources=[state_machine.state_machine_arn],
            )
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "TaskDef",
            family=f"video-migration-worker-{project_name}",
            cpu=1024,       # 1 vCPU
            memory_limit_mib=2048,  # 2 GB
            ephemeral_storage_gib=40,
            task_role=task_role,
        )

        log_group = logs.LogGroup(
            self,
            "WorkerLogs",
            log_group_name=f"/ecs/video-migration-worker-{project_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        task_definition.add_container(
            "Worker",
            image=ecs.ContainerImage.from_asset(WORKER_DIR),
            essential=True,
            environment={
                "QUEUE_URL": self.job_queue.queue_url,
                "STATE_TABLE_NAME": state_table.table_name,
                "MAPPING_TABLE_NAME": mapping_table.table_name,
                "STAGING_BUCKET": staging_bucket.bucket_name,
                "AWS_DEFAULT_REGION": cdk.Aws.REGION,
                "PROJECT_NAME": project_name,
            },
            secrets={
                "KALTURA_SECRET_ARN": ecs.Secret.from_secrets_manager(kaltura_secret),
                "ZOOM_SECRET_ARN": ecs.Secret.from_secrets_manager(zoom_secret),
            },
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="worker",
                log_group=log_group,
            ),
        )

        # ── Fargate Service ─────────────────────────────────────────
        self.fargate_service = ecs.FargateService(
            self,
            "WorkerService",
            service_name=f"video-migration-worker-{project_name}",
            cluster=self.cluster,
            task_definition=task_definition,
            desired_count=0,  # Scale from zero
            assign_public_ip=False,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            min_healthy_percent=0,
            max_healthy_percent=200,
        )

        # ── Auto-scaling based on SQS queue depth ───────────────────
        scaling = self.fargate_service.auto_scale_task_count(
            min_capacity=0,
            max_capacity=10,
        )

        scaling.scale_on_metric(
            "ScaleOnQueueDepth",
            metric=self.job_queue.metric_approximate_number_of_messages_visible(),
            scaling_steps=[
                appscaling.ScalingInterval(change=-1, upper=0),    # queue empty → 0 tasks
                appscaling.ScalingInterval(change=+1, lower=1),    # 1+ messages → scale up
                appscaling.ScalingInterval(change=+3, lower=10),   # 10+ → scale faster
                appscaling.ScalingInterval(change=+5, lower=50),   # 50+ → scale aggressive
            ],
            adjustment_type=appscaling.AdjustmentType.CHANGE_IN_CAPACITY,
        )

        # ── Outputs ─────────────────────────────────────────────────
        cdk.CfnOutput(self, "QueueUrl", value=self.job_queue.queue_url)
        cdk.CfnOutput(self, "QueueArn", value=self.job_queue.queue_arn)
        cdk.CfnOutput(self, "DlqUrl", value=self.dlq.queue_url)
        cdk.CfnOutput(self, "ClusterName", value=self.cluster.cluster_name)
