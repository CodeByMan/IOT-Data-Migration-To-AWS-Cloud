#!/opt/hackathon-kafka-admin/bin/python
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import boto3

REGION = "us-east-1"
INFRA_STACK = "CdkStack"
CLUSTER_NAME = "hackathon-msk-cluster"
SCRAM_SECRET = "AmazonMSK_hackathon_iot_rule"

RUNTIME_DIR = Path("/run/hackathon-kafka-connect")
STATE_DIR = Path("/var/lib/hackathon-kafka-connect")

WORKER_FILE = RUNTIME_DIR / "worker.properties"
CONNECTOR_FILE = RUNTIME_DIR / "jdbc-sink.properties"


def find_resource(
    resources: list[dict[str, Any]],
    resource_type: str,
    logical_text: str,
) -> str:
    matches = [
        item
        for item in resources
        if item.get("ResourceType") == resource_type
        and logical_text
        in str(item.get("LogicalResourceId", ""))
    ]

    if len(matches) != 1:
        raise RuntimeError(
            f"Could not resolve exactly one {logical_text}."
        )

    return str(matches[0]["PhysicalResourceId"])


def property_escape(value: Any) -> str:
    text = str(value)

    text = text.replace("\\", "\\\\")
    text = text.replace("\t", "\\t")
    text = text.replace("\n", "\\n")
    text = text.replace("\r", "\\r")
    text = text.replace("\f", "\\f")

    if text.startswith(" "):
        text = "\\" + text

    return text


def jaas_escape(value: Any) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "")
        .replace("\r", "")
    )


def write_properties(
    destination: Path,
    properties: list[tuple[str, Any]],
) -> None:
    temporary = destination.with_suffix(".tmp")

    lines = [
        f"{key}={property_escape(value)}"
        for key, value in properties
    ]

    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as file:
            file.write("\n".join(lines))
            file.write("\n")
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    os.replace(temporary, destination)
    os.chmod(destination, 0o600)


os.umask(0o077)
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

session = boto3.Session(region_name=REGION)

# ------------------------------------------------------------
# Resolve MSK and retrieve SCRAM credentials.
# ------------------------------------------------------------

kafka = session.client("kafka")

clusters = [
    item
    for item in kafka.list_clusters_v2(
        ClusterNameFilter=CLUSTER_NAME
    ).get("ClusterInfoList", [])
    if item.get("ClusterName") == CLUSTER_NAME
]

if len(clusters) != 1:
    raise RuntimeError("MSK cluster discovery failed.")

cluster = clusters[0]

if cluster.get("State") != "ACTIVE":
    raise RuntimeError("MSK cluster is not ACTIVE.")

brokers = kafka.get_bootstrap_brokers(
    ClusterArn=cluster["ClusterArn"]
).get("BootstrapBrokerStringSaslScram", "")

broker_endpoints = [
    endpoint.strip()
    for endpoint in brokers.split(",")
    if endpoint.strip()
]

if not broker_endpoints:
    raise RuntimeError("SCRAM bootstrap brokers are missing.")

if any(
    not endpoint.endswith(":9096")
    for endpoint in broker_endpoints
):
    raise RuntimeError("Unexpected SCRAM broker port.")

secrets = session.client("secretsmanager")

scram_response = secrets.get_secret_value(
    SecretId=SCRAM_SECRET
)

scram_credentials = json.loads(
    scram_response["SecretString"]
)

msk_username = scram_credentials.get("username")
msk_password = scram_credentials.get("password")

if not msk_username or not msk_password:
    raise RuntimeError("SCRAM credentials are incomplete.")

# ------------------------------------------------------------
# Resolve PostgreSQL and retrieve its credentials.
# ------------------------------------------------------------

cloudformation = session.client("cloudformation")

resources = cloudformation.describe_stack_resources(
    StackName=INFRA_STACK
).get("StackResources", [])

postgres_instance_id = find_resource(
    resources,
    "AWS::EC2::Instance",
    "PostgresInstance",
)

postgres_secret_id = find_resource(
    resources,
    "AWS::SecretsManager::Secret",
    "PostgresCredentials",
)

ec2 = session.client("ec2")

instance_response = ec2.describe_instances(
    InstanceIds=[postgres_instance_id]
)

instances = [
    instance
    for reservation
    in instance_response.get("Reservations", [])
    for instance in reservation.get("Instances", [])
]

if len(instances) != 1:
    raise RuntimeError("PostgreSQL instance discovery failed.")

postgres_instance = instances[0]

if postgres_instance.get("State", {}).get("Name") != "running":
    raise RuntimeError("PostgreSQL instance is not running.")

postgres_host = postgres_instance.get("PrivateDnsName")

if not postgres_host:
    raise RuntimeError("PostgreSQL private DNS is missing.")

postgres_response = secrets.get_secret_value(
    SecretId=postgres_secret_id
)

postgres_credentials = json.loads(
    postgres_response["SecretString"]
)

postgres_username = postgres_credentials.get("username")
postgres_password = postgres_credentials.get("password")
postgres_database = (
    postgres_credentials.get("dbname")
    or "onprem_iot"
)

if not postgres_username or not postgres_password:
    raise RuntimeError(
        "PostgreSQL credentials are incomplete."
    )

# ------------------------------------------------------------
# Create worker configuration.
# ------------------------------------------------------------

jaas_configuration = (
    "org.apache.kafka.common.security.scram."
    "ScramLoginModule required "
    f'username="{jaas_escape(msk_username)}" '
    f'password="{jaas_escape(msk_password)}";'
)

worker_properties = [
    ("bootstrap.servers", brokers),
    (
        "key.converter",
        "org.apache.kafka.connect.storage.StringConverter",
    ),
    (
        "value.converter",
        "org.apache.kafka.connect.json.JsonConverter",
    ),
    ("value.converter.schemas.enable", "true"),
    ("plugin.path", "/opt/kafka-plugins"),
    (
        "offset.storage.file.filename",
        str(STATE_DIR / "connect.offsets"),
    ),
    ("offset.flush.interval.ms", "10000"),
    ("listeners", "http://127.0.0.1:8083"),
    ("security.protocol", "SASL_SSL"),
    ("sasl.mechanism", "SCRAM-SHA-512"),
    ("sasl.jaas.config", jaas_configuration),
    ("consumer.security.protocol", "SASL_SSL"),
    ("consumer.sasl.mechanism", "SCRAM-SHA-512"),
    ("consumer.sasl.jaas.config", jaas_configuration),
    ("consumer.auto.offset.reset", "latest"),
]

write_properties(WORKER_FILE, worker_properties)

# ------------------------------------------------------------
# Create JDBC Sink connector configuration.
# ------------------------------------------------------------

connection_url = (
    f"jdbc:postgresql://{postgres_host}:5432/"
    f"{postgres_database}"
)

connector_properties = [
    ("name", "hackathon-jdbc-sink"),
    (
        "connector.class",
        "io.confluent.connect.jdbc.JdbcSinkConnector",
    ),
    ("tasks.max", "1"),
    ("topics", "iot-events"),
    ("connection.url", connection_url),
    ("connection.user", postgres_username),
    ("connection.password", postgres_password),
    ("dialect.name", "PostgreSqlDatabaseDialect"),
    ("insert.mode", "insert"),
    ("pk.mode", "none"),
    ("table.name.format", "public.iot_events"),
    ("auto.create", "false"),
    ("auto.evolve", "false"),
    ("db.timezone", "UTC"),
    ("errors.tolerance", "none"),
    ("transforms", "ConvertTimestamp,RenameTimestamp"),
    (
        "transforms.ConvertTimestamp.type",
        "org.apache.kafka.connect.transforms."
        "TimestampConverter$Value",
    ),
    (
        "transforms.ConvertTimestamp.field",
        "timestamp",
    ),
    (
        "transforms.ConvertTimestamp.target.type",
        "Timestamp",
    ),
    (
        "transforms.ConvertTimestamp.format",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    ),
    (
        "transforms.RenameTimestamp.type",
        "org.apache.kafka.connect.transforms."
        "ReplaceField$Value",
    ),
    (
        "transforms.RenameTimestamp.renames",
        "timestamp:event_time",
    ),
]

write_properties(CONNECTOR_FILE, connector_properties)

# Remove unnecessary in-memory references.
msk_username = None
msk_password = None
postgres_username = None
postgres_password = None

scram_credentials.clear()
postgres_credentials.clear()
scram_response.clear()
postgres_response.clear()

print("Runtime worker configuration generated: PASS")
print("Runtime connector configuration generated: PASS")
print("Secret values displayed: No")

# BEGIN HACKATHON DEBEZIUM GENERATION
import subprocess as _hackathon_debezium_subprocess

_hackathon_debezium_subprocess.run(
    ['/usr/local/sbin/hackathon-debezium-bootstrap'],
    check=True,
)
# END HACKATHON DEBEZIUM GENERATION

# BEGIN HACKATHON SOURCE PRODUCER SECURITY
import subprocess as _hackathon_source_producer_subprocess

_hackathon_source_producer_subprocess.run(
    ['/usr/local/sbin/hackathon-source-producer-security-bootstrap'],
    check=True,
)
# END HACKATHON SOURCE PRODUCER SECURITY
