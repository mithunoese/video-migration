"""Staging Bucket Stack — S3 bucket with KMS encryption and lifecycle rules.

Video files are temporarily staged here between Kaltura download and
Zoom upload.  Lifecycle rules automatically transition old objects to
Glacier and delete them after 90 days to control costs.
"""

import aws_cdk as cdk
from aws_cdk import aws_kms as kms, aws_s3 as s3
from constructs import Construct


class StagingBucketStack(cdk.Stack):

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

        self.bucket = s3.Bucket(
            self,
            "StagingBucket",
            bucket_name=f"video-migration-staging-{project_name}-{cdk.Aws.ACCOUNT_ID}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            bucket_key_enabled=True,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            auto_delete_objects=False,
            lifecycle_rules=[
                # Staging files → Glacier after 30 days
                s3.LifecycleRule(
                    id="TransitionToGlacier",
                    prefix="staging/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=cdk.Duration.days(30),
                        )
                    ],
                ),
                # Delete staging files after 90 days
                s3.LifecycleRule(
                    id="ExpireStaging",
                    prefix="staging/",
                    expiration=cdk.Duration.days(90),
                ),
                # Clean up old noncurrent versions
                s3.LifecycleRule(
                    id="ExpireNoncurrent",
                    noncurrent_version_expiration=cdk.Duration.days(7),
                ),
                # Reports retained longer — expire after 365 days
                s3.LifecycleRule(
                    id="ExpireReports",
                    prefix="reports/",
                    expiration=cdk.Duration.days(365),
                ),
            ],
        )

        # ── Outputs ─────────────────────────────────────────────────
        cdk.CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        cdk.CfnOutput(self, "BucketArn", value=self.bucket.bucket_arn)
