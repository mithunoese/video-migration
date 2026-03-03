"""Reusable KMS Customer Managed Key construct with automatic rotation."""

from constructs import Construct
import aws_cdk as cdk
import aws_cdk.aws_kms as kms


class MigrationKmsKey(Construct):
    """KMS CMK scoped to a single migration project.

    Features:
    - Automatic key rotation every 180 days
    - Alias namespaced by project
    - 30-day pending deletion window
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
    ) -> None:
        super().__init__(scope, construct_id)

        self.key = kms.Key(
            self,
            "Key",
            alias=f"alias/video-migration-{project_name}",
            description=f"Video migration CMK for project {project_name}",
            enable_key_rotation=True,
            rotation_period=cdk.Duration.days(180),
            pending_window=cdk.Duration.days(30),
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
