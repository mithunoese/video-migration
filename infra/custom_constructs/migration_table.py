"""Reusable DynamoDB table construct with KMS encryption and PITR."""

from constructs import Construct
import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_kms as kms


class MigrationTable(Construct):
    """DynamoDB table encrypted with a customer-managed KMS key.

    Features:
    - PAY_PER_REQUEST billing
    - Point-in-time recovery
    - KMS CMK encryption
    - Configurable removal policy
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        table_name: str,
        partition_key: dynamodb.Attribute,
        encryption_key: kms.IKey,
        stream: dynamodb.StreamViewType | None = None,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.RETAIN,
    ) -> None:
        super().__init__(scope, construct_id)

        self.table = dynamodb.Table(
            self,
            "Table",
            table_name=table_name,
            partition_key=partition_key,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=encryption_key,
            point_in_time_recovery=True,
            stream=stream,
            removal_policy=removal_policy,
        )
