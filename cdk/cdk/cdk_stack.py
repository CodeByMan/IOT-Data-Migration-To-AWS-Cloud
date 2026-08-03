import base64
import json
from pathlib import Path

from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_msk as msk
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


class CdkStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # -------------------------------------------------------------
        # VPC
        # -------------------------------------------------------------

        self.vpc = ec2.Vpc(
            self,
            "HackathonVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                ),
            ],
        )

        private_subnets = self.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
        ).subnets

        private_subnet_ids = [
            subnet.subnet_id
            for subnet in private_subnets
        ]

        # -------------------------------------------------------------
        # EC2 Systems Manager role
        # -------------------------------------------------------------

        self.ec2_ssm_role = iam.Role(
            self,
            "Ec2SsmRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description=(
                "IAM role for PostgreSQL and Bastion EC2 "
                "Session Manager access"
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore",
                ),
            ],
        )

        # -------------------------------------------------------------
        # Bastion and PostgreSQL security groups
        # -------------------------------------------------------------

        self.bastion_security_group = ec2.SecurityGroup(
            self,
            "BastionSecurityGroup",
            vpc=self.vpc,
            description="Security group for the Bastion Host",
            allow_all_outbound=True,
        )

        self.postgres_security_group = ec2.SecurityGroup(
            self,
            "PostgresSecurityGroup",
            vpc=self.vpc,
            description="Security group for PostgreSQL EC2",
            allow_all_outbound=True,
        )

        self.postgres_from_bastion_rule = (
            ec2.CfnSecurityGroupIngress(
                self,
                "PostgresFromBastion5432",
                group_id=(
                    self.postgres_security_group.security_group_id
                ),
                source_security_group_id=(
                    self.bastion_security_group.security_group_id
                ),
                ip_protocol="tcp",
                from_port=5432,
                to_port=5432,
                description=(
                    "Allow PostgreSQL access from the Bastion Host"
                ),
            )
        )

        # -------------------------------------------------------------
        # PostgreSQL credentials
        # -------------------------------------------------------------

        self.postgres_secret = secretsmanager.Secret(
            self,
            "PostgresCredentials",
            secret_name="hackathon/postgresql",
            description="Credentials for PostgreSQL EC2",
            generate_secret_string=(
                secretsmanager.SecretStringGenerator(
                    secret_string_template=json.dumps(
                        {
                            "username": "iot_admin",
                            "database": "onprem_iot",
                        }
                    ),
                    generate_string_key="password",
                    password_length=32,
                    exclude_punctuation=True,
                )
            ),
        )

        secret_read_grant = self.postgres_secret.grant_read(
            self.ec2_ssm_role
        )

        # -------------------------------------------------------------
        # Ubuntu Server 24.04 LTS
        # -------------------------------------------------------------

        ubuntu_2404_ami = ec2.MachineImage.from_ssm_parameter(
            parameter_name=(
                "/aws/service/canonical/ubuntu/server/noble/"
                "stable/current/amd64/hvm/ebs-gp3/ami-id"
            ),
            cached_in_context=True,
        )

        # -------------------------------------------------------------
        # Read the PostgreSQL initialization SQL at CDK synthesis time
        # -------------------------------------------------------------

        init_sql_path = (
            Path(__file__).resolve().parents[2]
            / "postgres"
            / "init.sql"
        )

        if not init_sql_path.is_file():
            raise FileNotFoundError(
                f"PostgreSQL initialization file not found: "
                f"{init_sql_path}"
            )

        init_sql_base64 = base64.b64encode(
            init_sql_path.read_bytes()
        ).decode("ascii")

        # -------------------------------------------------------------
        # PostgreSQL EC2 initialization
        # -------------------------------------------------------------

        postgres_user_data = ec2.UserData.custom(
            rf"""#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
    postgresql-16 \
    postgresql-client-16 \
    python3-boto3 \
    jq

systemctl enable postgresql
systemctl start postgresql

PG_HBA_CONF="/etc/postgresql/16/main/pg_hba.conf"

sudo -u postgres psql -v ON_ERROR_STOP=1 <<'SQL'
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET listen_addresses = '*';
ALTER SYSTEM SET password_encryption = 'scram-sha-256';
SQL

if ! grep -qF \
    "host all all {self.vpc.vpc_cidr_block} scram-sha-256" \
    "$PG_HBA_CONF"
then
    echo \
        "host all all {self.vpc.vpc_cidr_block} scram-sha-256" \
        >> "$PG_HBA_CONF"
fi

systemctl restart postgresql

export AWS_DEFAULT_REGION="{self.region}"
export SECRET_ARN="{self.postgres_secret.secret_arn}"

SECRET_JSON="$(python3 - <<'PY'
import boto3
import os

client = boto3.client(
    "secretsmanager",
    region_name=os.environ["AWS_DEFAULT_REGION"],
)

response = client.get_secret_value(
    SecretId=os.environ["SECRET_ARN"]
)

print(response["SecretString"])
PY
)"

DB_PASSWORD="$(
    printf '%s' "$SECRET_JSON" |
    jq -r '.password'
)"

if [ -z "$DB_PASSWORD" ] || [ "$DB_PASSWORD" = "null" ]
then
    echo "Database password was not retrieved." >&2
    exit 1
fi

sudo -u postgres psql \
    -v ON_ERROR_STOP=1 \
    --set=db_password="$DB_PASSWORD" <<'SQL'
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles
        WHERE rolname = 'iot_admin'
    ) THEN
        CREATE ROLE iot_admin LOGIN;
    END IF;
END
$do$;

ALTER ROLE iot_admin
    WITH LOGIN
    PASSWORD :'db_password';

SELECT 'CREATE DATABASE onprem_iot OWNER iot_admin'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'onprem_iot'
)\gexec
SQL

mkdir -p /opt/hackathon

echo "{init_sql_base64}" |
    base64 --decode \
    > /opt/hackathon/init.sql

        chown postgres:postgres /opt/hackathon/init.sql
        chmod 600 /opt/hackathon/init.sql

        sudo -u postgres psql \
    -v ON_ERROR_STOP=1 \
    -d onprem_iot \
    -f /opt/hackathon/init.sql

sudo -u postgres psql \
    -tAc "SHOW wal_level;" |
    grep -qx "logical"

sudo -u postgres psql \
    -d onprem_iot \
    -tAc \
    "SELECT to_regclass('public.iot_events');" |
    grep -qx "iot_events"

unset DB_PASSWORD
unset SECRET_JSON

touch /var/log/hackathon-postgres-ready
"""
        )

        # -------------------------------------------------------------
        # Bastion initialization
        # -------------------------------------------------------------

        bastion_user_data = ec2.UserData.custom(
            """#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
    postgresql-client-16 \
    python3-boto3 \
    jq

touch /var/log/hackathon-bastion-ready
"""
        )

        # -------------------------------------------------------------
        # PostgreSQL EC2
        # -------------------------------------------------------------

        self.postgres_instance = ec2.Instance(
            self,
            "PostgresInstance",
            instance_name="hackathon-postgresql",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=[private_subnets[0]],
            ),
            instance_type=ec2.InstanceType("t3.micro"),
            machine_image=ubuntu_2404_ami,
            security_group=self.postgres_security_group,
            role=self.ec2_ssm_role,
            user_data=postgres_user_data,
            associate_public_ip_address=False,
            require_imdsv2=True,
        )

        # -------------------------------------------------------------
        # Bastion EC2
        # -------------------------------------------------------------

        self.bastion_instance = ec2.Instance(
            self,
            "BastionInstance",
            instance_name="hackathon-bastion",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=[private_subnets[0]],
            ),
            instance_type=ec2.InstanceType("t3.micro"),
            machine_image=ubuntu_2404_ami,
            security_group=self.bastion_security_group,
            role=self.ec2_ssm_role,
            user_data=bastion_user_data,
            associate_public_ip_address=False,
            require_imdsv2=True,
        )

        secret_read_grant.apply_before(
            self.postgres_instance,
            self.bastion_instance,
        )

        # -------------------------------------------------------------
        # Amazon MSK
        # -------------------------------------------------------------

        self.msk_security_group = ec2.SecurityGroup(
            self,
            "MskSecurityGroup",
            vpc=self.vpc,
            description="Security group for the Amazon MSK cluster",
            allow_all_outbound=True,
        )

        self.msk_cluster = msk.CfnCluster(
            self,
            "HackathonMskCluster",
            cluster_name="hackathon-msk-cluster",
            kafka_version="3.9.x",
            number_of_broker_nodes=2,
            broker_node_group_info=(
                msk.CfnCluster.BrokerNodeGroupInfoProperty(
                    client_subnets=private_subnet_ids,
                    instance_type="kafka.t3.small",
                    security_groups=[
                        self.msk_security_group.security_group_id,
                    ],
                    connectivity_info=(
                        msk.CfnCluster.ConnectivityInfoProperty(
                            public_access=(
                                msk.CfnCluster.PublicAccessProperty(
                                    type="DISABLED",
                                )
                            ),
                        )
                    ),
                    storage_info=(
                        msk.CfnCluster.StorageInfoProperty(
                            ebs_storage_info=(
                                msk.CfnCluster.EBSStorageInfoProperty(
                                    volume_size=1,
                                )
                            ),
                        )
                    ),
                )
            ),
            client_authentication=(
                msk.CfnCluster.ClientAuthenticationProperty(
                    sasl=msk.CfnCluster.SaslProperty(
    iam=msk.CfnCluster.IamProperty(
        enabled=True,
    ),
    scram=msk.CfnCluster.ScramProperty(
        enabled=True,
    ),
),
                    unauthenticated=msk.CfnCluster.UnauthenticatedProperty(
                        enabled=False,
                    ),
                )
            ),
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=(
                    msk.CfnCluster.EncryptionInTransitProperty(
                        client_broker="TLS",
                        in_cluster=True,
                    )
                ),
            ),
        )