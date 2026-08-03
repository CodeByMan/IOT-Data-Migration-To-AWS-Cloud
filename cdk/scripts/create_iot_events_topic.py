from __future__ import annotations

import argparse
import base64
import json
import time

import boto3
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--region",
        required=True,
    )

    parser.add_argument(
        "--secret-id",
        required=True,
    )

    parser.add_argument(
        "--brokers-base64",
        required=True,
    )

    parser.add_argument(
        "--topic",
        required=True,
    )

    parser.add_argument(
        "--partitions",
        required=True,
        type=int,
    )

    parser.add_argument(
        "--replication-factor",
        required=True,
        type=int,
    )

    return parser.parse_args()


def retrieve_credentials(
    region: str,
    secret_id: str,
) -> tuple[str, str]:
    response = boto3.client(
        "secretsmanager",
        region_name=region,
    ).get_secret_value(
        SecretId=secret_id,
    )

    secret_object = json.loads(
        response["SecretString"]
    )

    username = str(
        secret_object.get(
            "username",
            "",
        )
    ).strip()

    password = str(
        secret_object.get(
            "password",
            "",
        )
    ).strip()

    secret_object.clear()
    response.clear()

    if not username or not password:
        raise RuntimeError(
            "SCRAM secret structure "
            "is invalid."
        )

    return username, password


def build_admin_client(
    brokers: str,
    username: str,
    password: str,
) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": brokers,
            "security.protocol": (
                "SASL_SSL"
            ),
            "sasl.mechanisms": (
                "SCRAM-SHA-512"
            ),
            "sasl.username": username,
            "sasl.password": password,
            "ssl.ca.location": (
                "/etc/ssl/certs/"
                "ca-certificates.crt"
            ),
            "socket.timeout.ms": 30000,
            "request.timeout.ms": 30000,
        }
    )


def validate_topic(
    admin_client: AdminClient,
    topic_name: str,
    partition_count: int,
    replication_factor: int,
) -> None:
    metadata = admin_client.list_topics(
        topic=topic_name,
        timeout=30,
    )

    topic_metadata = metadata.topics.get(
        topic_name
    )

    if topic_metadata is None:
        raise RuntimeError(
            "The topic was not found "
            "after creation."
        )

    if topic_metadata.error is not None:
        raise RuntimeError(
            "Kafka returned an error "
            "for the topic metadata."
        )

    partitions = topic_metadata.partitions

    if len(partitions) != partition_count:
        raise RuntimeError(
            "The topic partition count "
            "does not match the required "
            "implementation value."
        )

    for partition in partitions.values():
        if partition.error is not None:
            raise RuntimeError(
                "Kafka returned a partition "
                "metadata error."
            )

        if (
            len(partition.replicas)
            != replication_factor
        ):
            raise RuntimeError(
                "The topic replication factor "
                "does not match the required "
                "implementation value."
            )


def main() -> None:
    arguments = parse_arguments()

    if arguments.partitions < 1:
        raise ValueError(
            "Partition count must be "
            "at least one."
        )

    if arguments.replication_factor < 1:
        raise ValueError(
            "Replication factor must be "
            "at least one."
        )

    brokers = base64.b64decode(
        arguments.brokers_base64
    ).decode("utf-8")

    username, password = retrieve_credentials(
        arguments.region,
        arguments.secret_id,
    )

    admin_client = build_admin_client(
        brokers,
        username,
        password,
    )

    initial_metadata = (
        admin_client.list_topics(
            timeout=30
        )
    )

    topic_already_existed = (
        arguments.topic
        in initial_metadata.topics
        and initial_metadata.topics[
            arguments.topic
        ].error
        is None
    )

    if not topic_already_existed:
        topic_request = NewTopic(
            arguments.topic,
            num_partitions=(
                arguments.partitions
            ),
            replication_factor=(
                arguments.replication_factor
            ),
        )

        futures = admin_client.create_topics(
            [topic_request],
            operation_timeout=30,
            request_timeout=30,
        )

        try:
            futures[
                arguments.topic
            ].result(
                timeout=60
            )
        except KafkaException as error:
            kafka_error = error.args[0]

            if (
                not isinstance(
                    kafka_error,
                    KafkaError,
                )
                or kafka_error.code()
                != KafkaError.TOPIC_ALREADY_EXISTS
            ):
                raise

    for _ in range(12):
        try:
            validate_topic(
                admin_client,
                arguments.topic,
                arguments.partitions,
                arguments.replication_factor,
            )

            break
        except RuntimeError:
            time.sleep(5)
    else:
        validate_topic(
            admin_client,
            arguments.topic,
            arguments.partitions,
            arguments.replication_factor,
        )

    username = ""
    password = ""

    print(
        "TOPIC_EXISTED_BEFORE="
        + (
            "True"
            if topic_already_existed
            else "False"
        )
    )

    print(
        f"TOPIC_NAME={arguments.topic}"
    )

    print(
        "TOPIC_PARTITION_COUNT="
        f"{arguments.partitions}"
    )

    print(
        "TOPIC_REPLICATION_FACTOR="
        f"{arguments.replication_factor}"
    )

    print(
        "TOPIC_METADATA_VALIDATION=PASS"
    )

    print("IOT_EVENTS_TOPIC=PASS")


if __name__ == "__main__":
    main()
