"""Control Plane Stack — Step Functions state machine + Lambda functions.

The state machine orchestrates the full migration pipeline:
  Discover → Metadata → CreateZoomContainer → Map(Transfer jobs) → Reconcile

Each video transfer uses the .waitForTaskToken pattern: Step Functions
pauses and the Fargate worker calls SendTaskSuccess on completion.
"""

import os
import json

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_kms as kms,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


LAMBDA_HANDLERS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "lambda_handlers",
)


class ControlPlaneStack(cdk.Stack):

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
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.lambda_functions: list[lambda_.IFunction] = []

        # ── Shared Lambda execution role ────────────────────────────
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name=f"video-migration-lambda-{project_name}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )

        # Grant permissions to the role
        state_table.grant_read_write_data(lambda_role)
        mapping_table.grant_read_write_data(lambda_role)
        staging_bucket.grant_read_write(lambda_role)
        kaltura_secret.grant_read(lambda_role)
        zoom_secret.grant_read(lambda_role)
        kms_key.grant_encrypt_decrypt(lambda_role)

        # Common environment variables for all Lambdas
        common_env = {
            "PROJECT_NAME": project_name,
            "STATE_TABLE_NAME": state_table.table_name,
            "MAPPING_TABLE_NAME": mapping_table.table_name,
            "STAGING_BUCKET": staging_bucket.bucket_name,
            "KALTURA_SECRET_ARN": kaltura_secret.secret_arn,
            "ZOOM_SECRET_ARN": zoom_secret.secret_arn,
            "POWERTOOLS_SERVICE_NAME": "video-migration",
            "LOG_LEVEL": "INFO",
        }

        # ── Lambda: Discover ────────────────────────────────────────
        discover_fn = self._create_lambda(
            "DiscoverFn",
            handler_dir="discover",
            description="List all Kaltura videos and write manifest to S3",
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
            role=lambda_role,
            environment=common_env,
            vpc=vpc,
        )

        # ── Lambda: Extract Metadata ────────────────────────────────
        metadata_fn = self._create_lambda(
            "MetadataFn",
            handler_dir="extract_metadata",
            description="Pull metadata per video batch, write to S3",
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
            role=lambda_role,
            environment=common_env,
            vpc=vpc,
        )

        # ── Lambda: Create Zoom Container ───────────────────────────
        create_zoom_fn = self._create_lambda(
            "CreateZoomFn",
            handler_dir="create_zoom_container",
            description="Pre-create Zoom target container",
            timeout=cdk.Duration.minutes(1),
            memory_size=256,
            role=lambda_role,
            environment=common_env,
            vpc=vpc,
        )

        # ── Lambda: Verify Upload ───────────────────────────────────
        verify_fn = self._create_lambda(
            "VerifyFn",
            handler_dir="verify_upload",
            description="Verify Zoom upload success + checksum match",
            timeout=cdk.Duration.minutes(2),
            memory_size=256,
            role=lambda_role,
            environment=common_env,
            vpc=vpc,
        )

        # ── Lambda: Reconcile ───────────────────────────────────────
        reconcile_fn = self._create_lambda(
            "ReconcileFn",
            handler_dir="reconcile",
            description="Compare manifest vs DynamoDB, generate audit report",
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
            role=lambda_role,
            environment=common_env,
            vpc=vpc,
        )

        # ── Lambda: Update State ────────────────────────────────────
        update_state_fn = self._create_lambda(
            "UpdateStateFn",
            handler_dir="shared",
            handler_file="handler.update_state_handler",
            description="Update DynamoDB state + mapping table",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            role=lambda_role,
            environment=common_env,
            vpc=vpc,
        )

        # ── Step Functions State Machine ────────────────────────────
        self.state_machine = self._build_state_machine(
            project_name=project_name,
            discover_fn=discover_fn,
            metadata_fn=metadata_fn,
            create_zoom_fn=create_zoom_fn,
            verify_fn=verify_fn,
            reconcile_fn=reconcile_fn,
            update_state_fn=update_state_fn,
        )

        # Grant state machine permissions
        self.state_machine.grant_start_execution(lambda_role)

        # ── Outputs ─────────────────────────────────────────────────
        cdk.CfnOutput(
            self, "StateMachineArn", value=self.state_machine.state_machine_arn
        )

    def _create_lambda(
        self,
        construct_id: str,
        *,
        handler_dir: str,
        description: str,
        timeout: cdk.Duration,
        memory_size: int,
        role: iam.IRole,
        environment: dict,
        vpc: ec2.IVpc,
        handler_file: str = "handler.handler",
    ) -> lambda_.Function:
        """Create a Lambda function from a handler directory."""
        fn = lambda_.Function(
            self,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler=handler_file,
            code=lambda_.Code.from_asset(
                os.path.join(LAMBDA_HANDLERS_DIR, handler_dir)
            ),
            description=description,
            timeout=timeout,
            memory_size=memory_size,
            role=role,
            environment=environment,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            log_retention=logs.RetentionDays.TWO_WEEKS,
            tracing=lambda_.Tracing.ACTIVE,
        )
        self.lambda_functions.append(fn)
        return fn

    def _build_state_machine(
        self,
        *,
        project_name: str,
        discover_fn: lambda_.IFunction,
        metadata_fn: lambda_.IFunction,
        create_zoom_fn: lambda_.IFunction,
        verify_fn: lambda_.IFunction,
        reconcile_fn: lambda_.IFunction,
        update_state_fn: lambda_.IFunction,
    ) -> sfn.StateMachine:
        """Build the Step Functions state machine using CDK constructs."""

        # ── Step 1: Discover assets ─────────────────────────────────
        discover_step = sfn_tasks.LambdaInvoke(
            self,
            "DiscoverAssets",
            lambda_function=discover_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=True,
        )
        discover_step.add_retry(
            errors=["States.TaskFailed"],
            max_attempts=2,
            interval=cdk.Duration.seconds(5),
            backoff_rate=2.0,
        )

        # ── Step 2: Extract metadata ────────────────────────────────
        metadata_step = sfn_tasks.LambdaInvoke(
            self,
            "ExtractMetadata",
            lambda_function=metadata_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=True,
        )
        metadata_step.add_retry(
            errors=["States.TaskFailed"],
            max_attempts=2,
            interval=cdk.Duration.seconds(5),
            backoff_rate=2.0,
        )

        # ── Step 3: Create Zoom container ───────────────────────────
        create_zoom_step = sfn_tasks.LambdaInvoke(
            self,
            "CreateZoomContainer",
            lambda_function=create_zoom_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=True,
        )

        # ── Step 4a: Send to SQS (waitForTaskToken) ────────────────
        send_to_sqs = sfn_tasks.SqsSendMessage(
            self,
            "SendTransferJob",
            queue=sqs.Queue.from_queue_arn(
                self,
                "ImportedQueue",
                # This ARN is constructed at synth time; actual queue
                # is created in DataPlaneStack and referenced via
                # cross-stack output.
                queue_arn=f"arn:aws:sqs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:video-migration-jobs-{project_name}",
            ),
            message_body=sfn.TaskInput.from_object(
                {
                    "video_id": sfn.JsonPath.string_at("$.video_id"),
                    "manifest_key": sfn.JsonPath.string_at("$.manifest_key"),
                    "task_token": sfn.JsonPath.task_token,
                }
            ),
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            heartbeat=cdk.Duration.minutes(10),
            timeout=cdk.Duration.minutes(30),
        )

        # ── Step 4b: Verify upload ──────────────────────────────────
        verify_step = sfn_tasks.LambdaInvoke(
            self,
            "VerifyUpload",
            lambda_function=verify_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=True,
        )
        verify_step.add_retry(
            errors=["States.TaskFailed"],
            max_attempts=3,
            interval=cdk.Duration.seconds(10),
            backoff_rate=2.0,
        )

        # ── Step 4c: Update mapping ─────────────────────────────────
        update_mapping_step = sfn_tasks.LambdaInvoke(
            self,
            "UpdateMapping",
            lambda_function=update_state_fn,
            output_path="$.Payload",
        )

        # ── Step 4: Map state — process each video ──────────────────
        per_video_chain = send_to_sqs.next(verify_step).next(update_mapping_step)

        distribute_step = sfn.Map(
            self,
            "DistributeTransferJobs",
            items_path="$.video_ids",
            parameters={
                "video_id.$": "$$.Map.Item.Value",
                "manifest_key.$": "$.manifest_key",
            },
            max_concurrency=5,
        )
        distribute_step.item_processor(per_video_chain)

        # ── Step 5: Reconcile ───────────────────────────────────────
        reconcile_step = sfn_tasks.LambdaInvoke(
            self,
            "Reconcile",
            lambda_function=reconcile_fn,
            output_path="$.Payload",
        )

        # ── Error handler ───────────────────────────────────────────
        error_handler = sfn.Pass(self, "ErrorHandler")

        # ── Chain steps ─────────────────────────────────────────────
        definition = (
            discover_step
            .next(metadata_step)
            .next(create_zoom_step)
            .next(distribute_step)
            .next(reconcile_step)
        )

        # Add catch to every invoke step
        for step in [
            discover_step,
            metadata_step,
            create_zoom_step,
            distribute_step,
            reconcile_step,
        ]:
            step.add_catch(error_handler, errors=["States.ALL"])

        # ── Create state machine ────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "StateMachineLogGroup",
            log_group_name=f"/aws/vendedlogs/states/video-migration-{project_name}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        return sfn.StateMachine(
            self,
            "StateMachine",
            state_machine_name=f"VideoMigration-{project_name}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=cdk.Duration.hours(24),
            tracing_enabled=True,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
            ),
        )
