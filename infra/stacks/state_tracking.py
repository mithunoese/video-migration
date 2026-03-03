"""State Tracking Stack — DynamoDB tables for migration state and ID mapping.

Two tables:
1. State table — tracks every video through the pipeline (PENDING → COMPLETED)
2. Mapping table — deterministic SourceID → ZoomID lookup for reconciliation
"""

import aws_cdk as cdk
from aws_cdk import aws_dynamodb as dynamodb, aws_kms as kms
from constructs import Construct

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_constructs.migration_table import MigrationTable  # noqa: E402


class StateTrackingStack(cdk.Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        kms_key: kms.IKey,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Migration State Table ───────────────────────────────────
        state_construct = MigrationTable(
            self,
            "StateTable",
            table_name=f"video-migration-state-{project_name}",
            partition_key=dynamodb.Attribute(
                name="video_id", type=dynamodb.AttributeType.STRING
            ),
            encryption_key=kms_key,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        )
        self.state_table = state_construct.table

        # GSI: query by status (e.g., "show me all FAILED videos")
        self.state_table.add_global_secondary_index(
            index_name="status-index",
            partition_key=dynamodb.Attribute(
                name="status", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="updated_at", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ── Source → Zoom ID Mapping Table ──────────────────────────
        mapping_construct = MigrationTable(
            self,
            "MappingTable",
            table_name=f"video-id-mapping-{project_name}",
            partition_key=dynamodb.Attribute(
                name="source_id", type=dynamodb.AttributeType.STRING
            ),
            encryption_key=kms_key,
        )
        self.mapping_table = mapping_construct.table

        # ── Outputs ─────────────────────────────────────────────────
        cdk.CfnOutput(self, "StateTableName", value=self.state_table.table_name)
        cdk.CfnOutput(self, "StateTableArn", value=self.state_table.table_arn)
        cdk.CfnOutput(self, "MappingTableName", value=self.mapping_table.table_name)
