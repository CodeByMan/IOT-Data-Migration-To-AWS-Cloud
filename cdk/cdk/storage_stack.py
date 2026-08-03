from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct


class StorageStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.assignment_bucket = s3.Bucket(
            self,
            "AssignmentBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )

        CfnOutput(
            self,
            "AssignmentBucketName",
            value=self.assignment_bucket.bucket_name,
            description="Required Phase 1 S3 bucket",
        )