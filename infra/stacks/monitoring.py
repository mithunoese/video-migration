"""Monitoring Stack — CloudWatch dashboard, alarms, and SNS notifications.

Provides operational visibility into the migration pipeline:
- Real-time dashboard with SQS depth, Fargate tasks, Lambda errors
- Alarms that fire to an SNS topic for DLQ messages, execution failures,
  and Lambda error rates
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_dynamodb as dynamodb,
    aws_ecs as ecs,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
)
from constructs import Construct


class MonitoringStack(cdk.Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        state_table: dynamodb.ITable,
        staging_bucket: s3.IBucket,
        job_queue: sqs.IQueue,
        dlq: sqs.IQueue,
        state_machine: sfn.IStateMachine,
        ecs_cluster: ecs.ICluster,
        fargate_service: ecs.FargateService,
        lambda_functions: list[lambda_.IFunction],
        alarm_email: str = "",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── SNS Topic for alarms ────────────────────────────────────
        alarm_topic = sns.Topic(
            self,
            "AlarmTopic",
            topic_name=f"video-migration-alarms-{project_name}",
            display_name=f"Video Migration Alarms ({project_name})",
        )

        if alarm_email:
            alarm_topic.add_subscription(
                sns_subs.EmailSubscription(alarm_email)
            )

        alarm_action = cw_actions.SnsAction(alarm_topic)

        # ── CloudWatch Dashboard ────────────────────────────────────
        dashboard = cw.Dashboard(
            self,
            "Dashboard",
            dashboard_name=f"VideoMigration-{project_name}",
            period_override=cw.PeriodOverride.AUTO,
        )

        # Row 1: Pipeline overview
        dashboard.add_widgets(
            cw.GraphWidget(
                title="SQS Queue Depth",
                left=[
                    job_queue.metric_approximate_number_of_messages_visible(
                        period=cdk.Duration.minutes(1)
                    ),
                    job_queue.metric_approximate_number_of_messages_not_visible(
                        period=cdk.Duration.minutes(1)
                    ),
                ],
                width=8,
            ),
            cw.GraphWidget(
                title="DLQ Messages",
                left=[
                    dlq.metric_approximate_number_of_messages_visible(
                        period=cdk.Duration.minutes(1)
                    ),
                ],
                width=4,
            ),
            cw.GraphWidget(
                title="Active Fargate Tasks",
                left=[
                    fargate_service.metric_cpu_utilization(
                        period=cdk.Duration.minutes(1)
                    ),
                ],
                right=[
                    cw.Metric(
                        namespace="ECS/ContainerInsights",
                        metric_name="RunningTaskCount",
                        dimensions_map={
                            "ClusterName": ecs_cluster.cluster_name,
                            "ServiceName": fargate_service.service_name,
                        },
                        period=cdk.Duration.minutes(1),
                        statistic="Average",
                    ),
                ],
                width=6,
            ),
            cw.GraphWidget(
                title="Step Functions Executions",
                left=[
                    state_machine.metric_started(period=cdk.Duration.minutes(5)),
                    state_machine.metric_succeeded(period=cdk.Duration.minutes(5)),
                    state_machine.metric_failed(period=cdk.Duration.minutes(5)),
                    state_machine.metric_timed_out(period=cdk.Duration.minutes(5)),
                ],
                width=6,
            ),
        )

        # Row 2: Lambda metrics
        if lambda_functions:
            error_metrics = []
            duration_metrics = []
            for fn in lambda_functions:
                error_metrics.append(
                    fn.metric_errors(period=cdk.Duration.minutes(5))
                )
                duration_metrics.append(
                    fn.metric_duration(period=cdk.Duration.minutes(5))
                )

            dashboard.add_widgets(
                cw.GraphWidget(
                    title="Lambda Errors",
                    left=error_metrics,
                    width=12,
                ),
                cw.GraphWidget(
                    title="Lambda Duration",
                    left=duration_metrics,
                    width=12,
                ),
            )

        # Row 3: DynamoDB and S3
        dashboard.add_widgets(
            cw.GraphWidget(
                title="DynamoDB Read/Write Units",
                left=[
                    state_table.metric_consumed_read_capacity_units(
                        period=cdk.Duration.minutes(5)
                    ),
                    state_table.metric_consumed_write_capacity_units(
                        period=cdk.Duration.minutes(5)
                    ),
                ],
                width=12,
            ),
            cw.SingleValueWidget(
                title="S3 Bucket Size",
                metrics=[
                    cw.Metric(
                        namespace="AWS/S3",
                        metric_name="BucketSizeBytes",
                        dimensions_map={
                            "BucketName": staging_bucket.bucket_name,
                            "StorageType": "StandardStorage",
                        },
                        period=cdk.Duration.days(1),
                        statistic="Average",
                    ),
                ],
                width=6,
            ),
            cw.SingleValueWidget(
                title="S3 Object Count",
                metrics=[
                    cw.Metric(
                        namespace="AWS/S3",
                        metric_name="NumberOfObjects",
                        dimensions_map={
                            "BucketName": staging_bucket.bucket_name,
                            "StorageType": "AllStorageTypes",
                        },
                        period=cdk.Duration.days(1),
                        statistic="Average",
                    ),
                ],
                width=6,
            ),
        )

        # ── Alarms ──────────────────────────────────────────────────

        # DLQ has messages → something failed 3x
        dlq_alarm = cw.Alarm(
            self,
            "DlqAlarm",
            alarm_name=f"video-migration-{project_name}-dlq-messages",
            alarm_description="Dead-letter queue has messages — transfer failures",
            metric=dlq.metric_approximate_number_of_messages_visible(
                period=cdk.Duration.minutes(1)
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        dlq_alarm.add_alarm_action(alarm_action)

        # Step Functions execution failed
        sfn_fail_alarm = cw.Alarm(
            self,
            "SfnFailAlarm",
            alarm_name=f"video-migration-{project_name}-sfn-failed",
            alarm_description="Step Functions execution failed",
            metric=state_machine.metric_failed(period=cdk.Duration.minutes(5)),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        sfn_fail_alarm.add_alarm_action(alarm_action)

        # Lambda error rate > 5%
        for fn in lambda_functions:
            fn_name = fn.function_name
            error_alarm = cw.Alarm(
                self,
                f"LambdaErrorAlarm-{fn.node.id}",
                alarm_name=f"video-migration-{project_name}-lambda-errors-{fn.node.id}",
                alarm_description=f"Lambda {fn.node.id} error rate > 5%",
                metric=cw.MathExpression(
                    expression="errors / invocations * 100",
                    using_metrics={
                        "errors": fn.metric_errors(period=cdk.Duration.minutes(5)),
                        "invocations": fn.metric_invocations(
                            period=cdk.Duration.minutes(5)
                        ),
                    },
                    period=cdk.Duration.minutes(5),
                ),
                threshold=5,
                evaluation_periods=2,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            )
            error_alarm.add_alarm_action(alarm_action)

        # ── Outputs ─────────────────────────────────────────────────
        cdk.CfnOutput(self, "DashboardUrl", value=dashboard.dashboard_arn)
        cdk.CfnOutput(self, "AlarmTopicArn", value=alarm_topic.topic_arn)
