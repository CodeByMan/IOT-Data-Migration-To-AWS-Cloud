#!/usr/bin/env python3

import os

import aws_cdk as cdk

from cdk.cdk_stack import CdkStack
from cdk.kafka_connect_stack import KafkaConnectStack
from cdk.storage_stack import StorageStack
from cdk.topic_admin_stack import TopicAdminPrerequisitesStack


app = cdk.App()

environment = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

CdkStack(
    app,
    "CdkStack",
    env=environment,
)

StorageStack(
    app,
    "StorageStack",
    env=environment,
)

TopicAdminPrerequisitesStack(
    app,
    "TopicAdminPrerequisitesStack",
    env=environment,
)

KafkaConnectStack(
    app,
    "KafkaConnectStack",
    env=environment,
)

app.synth()
