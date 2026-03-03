"""Shared Infrastructure Stack — VPC, KMS CMK, Secrets Manager secrets.

Every migration project gets its own KMS key and credential secrets.
The VPC provides network isolation for Fargate workers with VPC
endpoints to avoid NAT gateway costs for AWS service traffic.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_constructs.kms_key import MigrationKmsKey  # noqa: E402


class SharedInfraStack(cdk.Stack):
    """Creates VPC, KMS CMK, and Secrets Manager secrets for one project."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        project_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── KMS Customer Managed Key ────────────────────────────────
        cmk = MigrationKmsKey(self, "CMK", project_name=project_name)
        self.kms_key: kms.IKey = cmk.key

        # ── VPC with private subnets + NAT ──────────────────────────
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name=f"video-migration-{project_name}",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
            ],
        )

        # ── VPC Endpoints (avoid NAT costs for AWS service calls) ───
        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )
        self.vpc.add_gateway_endpoint(
            "DynamoEndpoint",
            service=ec2.GatewayVpcEndpointAwsService.DYNAMODB,
        )
        self.vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
        )
        self.vpc.add_interface_endpoint(
            "SqsEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SQS,
        )
        self.vpc.add_interface_endpoint(
            "StepFunctionsEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.STEP_FUNCTIONS,
        )

        # ── Secrets Manager — Kaltura credentials ───────────────────
        self.kaltura_secret = secretsmanager.Secret(
            self,
            "KalturaSecret",
            secret_name=f"video-migration/{project_name}/kaltura",
            description=f"Kaltura API credentials for {project_name}",
            encryption_key=self.kms_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"partner_id":"","admin_secret":"","user_id":""}',
                generate_string_key="_placeholder",
            ),
        )

        # ── Secrets Manager — Zoom credentials ─────────────────────
        self.zoom_secret = secretsmanager.Secret(
            self,
            "ZoomSecret",
            secret_name=f"video-migration/{project_name}/zoom",
            description=f"Zoom S2S OAuth credentials for {project_name}",
            encryption_key=self.kms_key,
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"client_id":"","client_secret":"","account_id":""}',
                generate_string_key="_placeholder",
            ),
        )

        # ── Outputs ─────────────────────────────────────────────────
        cdk.CfnOutput(self, "KmsKeyArn", value=self.kms_key.key_arn)
        cdk.CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        cdk.CfnOutput(
            self, "KalturaSecretArn", value=self.kaltura_secret.secret_arn
        )
        cdk.CfnOutput(
            self, "ZoomSecretArn", value=self.zoom_secret.secret_arn
        )
