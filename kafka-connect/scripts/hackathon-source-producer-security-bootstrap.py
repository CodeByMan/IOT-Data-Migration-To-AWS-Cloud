#!/usr/bin/python3

from __future__ import annotations

import os
import tempfile
from pathlib import Path

WORKER = Path(
    "/run/hackathon-kafka-connect/worker.properties"
)

MAPPINGS = [
    (
        "security.protocol",
        "producer.security.protocol",
    ),
    (
        "sasl.mechanism",
        "producer.sasl.mechanism",
    ),
    (
        "sasl.jaas.config",
        "producer.sasl.jaas.config",
    ),
]


def fail(message: str) -> None:
    raise SystemExit(message)


if not WORKER.is_file():
    fail("Kafka Connect worker properties are missing.")

current_mode = WORKER.stat().st_mode & 0o777

if current_mode & 0o077:
    fail(
        "Kafka Connect worker properties "
        "permissions are too broad."
    )

original_lines = WORKER.read_text(
    encoding="utf-8",
).splitlines()

properties: dict[str, str] = {}

for raw_line in original_lines:
    line = raw_line.strip()

    if not line or line.startswith(("#", "!")):
        continue

    if "=" not in line:
        continue

    key, value = line.split("=", 1)

    properties[key.strip()] = value.strip()

for source_key, _ in MAPPINGS:
    if not properties.get(source_key):
        fail(
            f"Required worker property "
            f"{source_key} is missing."
        )

producer_keys = {
    target_key
    for _, target_key in MAPPINGS
}

retained_lines = []

for raw_line in original_lines:
    stripped = raw_line.strip()

    if "=" in stripped:
        key = stripped.split("=", 1)[0].strip()

        if key in producer_keys:
            continue

    retained_lines.append(raw_line)

while (
    retained_lines
    and retained_lines[-1] == ""
):
    retained_lines.pop()

retained_lines.append("")

for source_key, target_key in MAPPINGS:
    retained_lines.append(
        f"{target_key}={properties[source_key]}"
    )

content = "\n".join(retained_lines) + "\n"

descriptor, temporary_name = tempfile.mkstemp(
    prefix=".worker-producer-security.",
    suffix=".tmp",
    dir=str(WORKER.parent),
    text=True,
)

try:
    os.fchmod(descriptor, 0o600)

    with os.fdopen(
        descriptor,
        "w",
        encoding="utf-8",
    ) as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())

    os.replace(
        temporary_name,
        WORKER,
    )

    os.chmod(WORKER, 0o600)

except BaseException:
    try:
        os.unlink(temporary_name)
    except FileNotFoundError:
        pass

    raise

print(
    "Kafka source-producer security "
    "properties generated."
)
