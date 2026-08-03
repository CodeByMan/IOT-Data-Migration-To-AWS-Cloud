import base64

from aws_cdk import CfnCondition, CfnParameter, Fn, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kafkaconnect as kafkaconnect
from constructs import Construct


class KafkaConnectStack(Stack):
    """Optional managed MSK Connect JDBC Sink implementation."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Existing infrastructure is supplied as deployment parameters so this
        # stack can be deployed independently without updating CdkStack.
        vpc_id = CfnParameter(
            self,
            "VpcId",
            type="AWS::EC2::VPC::Id",
        )

        private_subnet_ids = CfnParameter(
            self,
            "PrivateSubnetIds",
            type="List<AWS::EC2::Subnet::Id>",
        )

        msk_cluster_arn = CfnParameter(
            self,
            "MskClusterArn",
            type="String",
            no_echo=True,
        )

        msk_bootstrap_servers_iam = CfnParameter(
            self,
            "MskBootstrapServersIam",
            type="String",
            no_echo=True,
        )

        msk_security_group_id = CfnParameter(
            self,
            "MskSecurityGroupId",
            type="AWS::EC2::SecurityGroup::Id",
        )

        postgres_security_group_id = CfnParameter(
            self,
            "PostgresSecurityGroupId",
            type="AWS::EC2::SecurityGroup::Id",
        )

        postgres_private_dns = CfnParameter(
            self,
            "PostgresPrivateDns",
            type="String",
            no_echo=True,
        )

        postgres_secret_arn = CfnParameter(
            self,
            "PostgresSecretArn",
            type="String",
            no_echo=True,
        )

        postgres_secret_name = CfnParameter(
            self,
            "PostgresSecretName",
            type="String",
            no_echo=True,
        )

        assignment_bucket_name = CfnParameter(
            self,
            "AssignmentBucketName",
            type="String",
            no_echo=True,
        )

        plugin_object_key = CfnParameter(
            self,
            "PluginObjectKey",
            type="String",
            default=(
                "msk-connect/plugins/"
                "hackathon-jdbc-secrets-plugin.zip"
            ),
        )

        create_connector_now = CfnParameter(
            self,
            "CreateConnectorNow",
            type="String",
            default="false",
            allowed_values=["false", "true"],
            description=(
                "Deploy prerequisites first; set true only on the "
                "second deployment to create the connector"
            ),
        )

        create_connector_condition = CfnCondition(
            self,
            "CreateConnectorCondition",
            expression=Fn.condition_equals(
                create_connector_now.value_as_string,
                "true",
            ),
        )

        connector_security_group = ec2.CfnSecurityGroup(
            self,
            "KafkaConnectSecurityGroup",
            group_description=(
                "Security group for the Managed MSK Connect implementation"
            ),
            vpc_id=vpc_id.value_as_string,
        )

        ec2.CfnSecurityGroupIngress(
            self,
            "MskIamFromKafkaConnect9098",
            group_id=msk_security_group_id.value_as_string,
            source_security_group_id=connector_security_group.attr_group_id,
            ip_protocol="tcp",
            from_port=9098,
            to_port=9098,
            description=(
                "Allow MSK Connect to reach the MSK IAM TLS listener"
            ),
        )

        ec2.CfnSecurityGroupIngress(
            self,
            "PostgresFromKafkaConnect5432",
            group_id=postgres_security_group_id.value_as_string,
            source_security_group_id=connector_security_group.attr_group_id,
            ip_protocol="tcp",
            from_port=5432,
            to_port=5432,
            description=(
                "Allow the JDBC sink to write to private PostgreSQL"
            ),
        )

        connector_role = iam.Role(
            self,
            "KafkaConnectExecutionRole",
            assumed_by=iam.ServicePrincipal(
                "kafkaconnect.amazonaws.com"
            ),
            description=(
                "Service execution role for the Managed MSK Connect implementation"
            ),
        )

        cluster_arn_prefix = Fn.select(
            0,
            Fn.split(
                ":cluster/",
                msk_cluster_arn.value_as_string,
            ),
        )
        cluster_resource_suffix = Fn.select(
            1,
            Fn.split(
                ":cluster/",
                msk_cluster_arn.value_as_string,
            ),
        )

        topic_resource_prefix = Fn.join(
            "",
            [
                cluster_arn_prefix,
                ":topic/",
                cluster_resource_suffix,
                "/",
            ],
        )
        group_resource_prefix = Fn.join(
            "",
            [
                cluster_arn_prefix,
                ":group/",
                cluster_resource_suffix,
                "/",
            ],
        )

        connector_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:Connect",
                    "kafka-cluster:DescribeCluster",
                ],
                resources=[msk_cluster_arn.value_as_string],
            )
        )

        connector_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:ReadData",
                    "kafka-cluster:DescribeTopic",
                ],
                resources=[
                    Fn.join(
                        "",
                        [topic_resource_prefix, "iot-events"],
                    )
                ],
            )
        )

        connector_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:CreateTopic",
                    "kafka-cluster:WriteData",
                    "kafka-cluster:ReadData",
                    "kafka-cluster:DescribeTopic",
                ],
                resources=[
                    Fn.join(
                        "",
                        [
                            topic_resource_prefix,
                            "__amazon_msk_connect_*",
                        ],
                    ),
                    Fn.join(
                        "",
                        [
                            topic_resource_prefix,
                            "__msk_connect_*",
                        ],
                    ),
                ],
            )
        )

        connector_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:AlterGroup",
                    "kafka-cluster:DescribeGroup",
                ],
                resources=[
                    Fn.join(
                        "",
                        [
                            group_resource_prefix,
                            "__amazon_msk_connect_*",
                        ],
                    ),
                    Fn.join(
                        "",
                        [
                            group_resource_prefix,
                            "__msk_connect_*",
                        ],
                    ),
                    Fn.join(
                        "",
                        [group_resource_prefix, "connect-*"],
                    ),
                ],
            )
        )

        connector_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "secretsmanager:GetResourcePolicy",
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                    "secretsmanager:ListSecretVersionIds",
                ],
                resources=[postgres_secret_arn.value_as_string],
            )
        )

        bucket_arn = Fn.sub(
            "arn:${AWS::Partition}:s3:::${BucketName}",
            {
                "BucketName": (
                    assignment_bucket_name.value_as_string
                )
            },
        )

        plugin_object_arn = Fn.join(
            "",
            [
                bucket_arn,
                "/",
                plugin_object_key.value_as_string,
            ],
        )

        connector_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[plugin_object_arn],
            )
        )

        jdbc_plugin = kafkaconnect.CfnCustomPlugin(
            self,
            "JdbcCustomPlugin",
            content_type="ZIP",
            location=(
                kafkaconnect.CfnCustomPlugin.CustomPluginLocationProperty(
                    s3_location=(
                        kafkaconnect.CfnCustomPlugin.S3LocationProperty(
                            bucket_arn=bucket_arn,
                            file_key=plugin_object_key.value_as_string,
                        )
                    )
                )
            ),
            name="hackathon-jdbc-secrets-plugin",
            description=(
                "JDBC sink and Secrets Manager configuration provider"
            ),
        )

        worker_properties = "\n".join(
            [
                (
                    "key.converter="
                    "org.apache.kafka.connect.storage.StringConverter"
                ),
                "key.converter.schemas.enable=false",
                (
                    "value.converter="
                    "org.apache.kafka.connect.json.JsonConverter"
                ),
                "value.converter.schemas.enable=true",
                "consumer.auto.offset.reset=latest",
                "config.providers=secretsmanager",
                (
                    "config.providers.secretsmanager.class="
                    "com.amazonaws.kafka.config.providers."
                    "SecretsManagerConfigProvider"
                ),
                (
                    "config.providers.secretsmanager.param.region="
                    "us-east-1"
                ),
                "",
            ]
        )

        worker_configuration = (
            kafkaconnect.CfnWorkerConfiguration(
                self,
                "JdbcWorkerConfiguration",
                name="hackathon-jdbc-worker",
                description=(
                    "Schema-aware JSON worker configuration for JDBC sink"
                ),
                properties_file_content=(
                    base64.b64encode(
                        worker_properties.encode("utf-8")
                    ).decode("ascii")
                ),
            )
        )

        def secret_provider_reference(json_field: str) -> str:
            return Fn.join(
                "",
                [
                    "${secretsmanager:",
                    postgres_secret_name.value_as_string,
                    ":",
                    json_field,
                    "}",
                ],
            )

        connection_url = Fn.join(
            "",
            [
                "jdbc:postgresql://",
                postgres_private_dns.value_as_string,
                ":5432/onprem_iot",
            ],
        )

        connector = kafkaconnect.CfnConnector(
            self,
            "JdbcSinkConnector",
            capacity=kafkaconnect.CfnConnector.CapacityProperty(
                provisioned_capacity=(
                    kafkaconnect.CfnConnector.ProvisionedCapacityProperty(
                        mcu_count=1,
                        worker_count=1,
                    )
                )
            ),
            connector_configuration={
                "connector.class": (
                    "io.confluent.connect.jdbc.JdbcSinkConnector"
                ),
                "tasks.max": "1",
                "topics": "iot-events",
                "connection.url": connection_url,
                "connection.user": secret_provider_reference("username"),
                "connection.password": secret_provider_reference("password"),
                "config.action.reload": "none",
                "dialect.name": "PostgreSqlDatabaseDialect",
                "insert.mode": "insert",
                "pk.mode": "none",
                "table.name.format": "public.iot_events",
                "auto.create": "false",
                "auto.evolve": "false",
                "db.timezone": "UTC",
                "transforms": "ConvertTimestamp,RenameTimestamp",
                "transforms.ConvertTimestamp.type": (
                    "org.apache.kafka.connect.transforms."
                    "TimestampConverter$Value"
                ),
                "transforms.ConvertTimestamp.field": "timestamp",
                "transforms.ConvertTimestamp.target.type": "Timestamp",
                "transforms.ConvertTimestamp.format": (
                    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
                ),
                "transforms.RenameTimestamp.type": (
                    "org.apache.kafka.connect.transforms."
                    "ReplaceField$Value"
                ),
                "transforms.RenameTimestamp.renames": (
                    "timestamp:event_time"
                ),
            },
            connector_description=(
    "Optional managed MSK Connect JDBC Sink from "
    "iot-events to PostgreSQL"
),
            connector_name="hackathon-jdbc-sink",
            kafka_cluster=(
                kafkaconnect.CfnConnector.KafkaClusterProperty(
                    apache_kafka_cluster=(
                        kafkaconnect.CfnConnector.ApacheKafkaClusterProperty(
                            bootstrap_servers=(
                                msk_bootstrap_servers_iam.value_as_string
                            ),
                            vpc=kafkaconnect.CfnConnector.VpcProperty(
                                security_groups=[
                                    connector_security_group.attr_group_id
                                ],
                                subnets=(
                                    private_subnet_ids.value_as_list
                                ),
                            ),
                        )
                    )
                )
            ),
            kafka_cluster_client_authentication=(
                kafkaconnect.CfnConnector.KafkaClusterClientAuthenticationProperty(
                    authentication_type="IAM"
                )
            ),
            kafka_cluster_encryption_in_transit=(
                kafkaconnect.CfnConnector.KafkaClusterEncryptionInTransitProperty(
                    encryption_type="TLS"
                )
            ),
            kafka_connect_version="3.7.x",
            plugins=[
                kafkaconnect.CfnConnector.PluginProperty(
                    custom_plugin=(
                        kafkaconnect.CfnConnector.CustomPluginProperty(
                            custom_plugin_arn=(
                                jdbc_plugin.attr_custom_plugin_arn
                            ),
                            revision=1,
                        )
                    )
                )
            ],
            service_execution_role_arn=connector_role.role_arn,
            worker_configuration=(
                kafkaconnect.CfnConnector.WorkerConfigurationProperty(
                    revision=worker_configuration.attr_revision,
                    worker_configuration_arn=(
                        worker_configuration.attr_worker_configuration_arn
                    ),
                )
            ),
        )

        connector.cfn_options.condition = create_connector_condition

        connector.add_resource_dependency(jdbc_plugin)
        connector.add_resource_dependency(worker_configuration)

        default_policy = connector_role.node.try_find_child("DefaultPolicy")
        if default_policy is not None:
            connector.node.add_dependency(default_policy)
