#!/usr/bin/python3

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

RUNTIME_DIR = Path("/run/hackathon-kafka-connect")
SOURCE = RUNTIME_DIR / "jdbc-sink.properties"
TARGET = RUNTIME_DIR / "debezium-source.properties"


def fail(message: str) -> None:
    raise SystemExit(message)


def raw_property(
    lines: list[str],
    key: str,
) -> str:
    prefix = key + "="

    values = [
        line[len(prefix):]
        for line in lines
        if line.startswith(prefix)
    ]

    if len(values) != 1:
        fail(
            f"Expected exactly one {key} property."
        )

    if values[0] == "":
        fail(f"{key} is empty.")

    return values[0]


if not SOURCE.is_file():
    fail("JDBC Sink properties file is missing.")

source_mode = SOURCE.stat().st_mode & 0o777

if source_mode & 0o077:
    fail(
        "JDBC Sink properties permissions "
        "are too broad."
    )

lines = SOURCE.read_text(
    encoding="utf-8",
).splitlines()

jdbc_url = raw_property(
    lines,
    "connection.url",
)

database_user = raw_property(
    lines,
    "connection.user",
)

database_password = raw_property(
    lines,
    "connection.password",
)

prefix = "jdbc:postgresql://"

if not jdbc_url.startswith(prefix):
    fail(
        "The JDBC URL is not PostgreSQL."
    )

parsed = urlsplit(
    jdbc_url[len("jdbc:"):]
)

hostname = parsed.hostname
port = parsed.port or 5432
database = parsed.path.lstrip("/")

if not hostname:
    fail(
        "PostgreSQL hostname could not be parsed."
    )

if not database:
    fail(
        "PostgreSQL database could not be parsed."
    )

properties = [
    "name=hackathon-debezium-postgres-source",
    (
        "connector.class="
        "io.debezium.connector.postgresql."
        "PostgresConnector"
    ),
    "tasks.max=1",
    f"database.hostname={hostname}",
    f"database.port={port}",
    f"database.user={database_user}",
    f"database.password={database_password}",
    f"database.dbname={database}",
    "topic.prefix=cdc",
    "plugin.name=pgoutput",
    "slot.name=hackathon_debezium_slot",
    (
        "publication.name="
        "hackathon_debezium_publication"
    ),
    "publication.autocreate.mode=filtered",
    "table.include.list=public.iot_events",
    "snapshot.mode=no_data",
    "tombstones.on.delete=false",
    "slot.drop.on.stop=false",
]

content = "\n".join(properties) + "\n"

file_descriptor, temporary_name = (
    tempfile.mkstemp(
        prefix=".debezium-source.",
        suffix=".tmp",
        dir=RUNTIME_DIR,
        text=True,
    )
)

try:
    os.fchmod(file_descriptor, 0o600)

    with os.fdopen(
        file_descriptor,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(content)
        file.flush()
        os.fsync(file.fileno())

    os.replace(
        temporary_name,
        TARGET,
    )

    os.chmod(
        TARGET,
        0o600,
    )

except BaseException:
    try:
        os.unlink(temporary_name)
    except FileNotFoundError:
        pass

    raise

print(
    "Secure Debezium runtime "
    "configuration generated."
)
