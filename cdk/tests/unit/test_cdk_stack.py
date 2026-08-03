from __future__ import annotations

import os

import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk.cdk_stack import CdkStack


def test_core_infrastructure_resources() -> None:
    account = os.getenv("CDK_DEFAULT_ACCOUNT", "111111111111")
    region = os.getenv("CDK_DEFAULT_REGION", "us-east-1")

    app = core.App()

    stack = CdkStack(
        app,
        "CdkStackTest",
        env=core.Environment(
            account=account,
            region=region,
        ),
    )

    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::MSK::Cluster", 1)
    template.resource_count_is("AWS::EC2::Instance", 2)
    template.resource_count_is(
        "AWS::SecretsManager::Secret",
        1,
    )