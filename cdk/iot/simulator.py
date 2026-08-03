from __future__ import annotations

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from awscrt import mqtt
from awsiot import mqtt_connection_builder


SCRIPT_DIR = Path(__file__).resolve().parent
CERTIFICATE_DIRECTORY = SCRIPT_DIR / "certificates"
MANIFEST_PATH = CERTIFICATE_DIRECTORY / "manifest.json"
ROOT_CA_PATH = CERTIFICATE_DIRECTORY / "AmazonRootCA1.pem"
MQTT_TOPIC = "iot/geolocation"
DEVICE_COUNT = 5


def load_manifest() -> list[dict[str, Any]]:
    with MANIFEST_PATH.open("r", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    if not isinstance(manifest, list):
        raise RuntimeError("The certificate manifest must be a JSON array.")

    if len(manifest) != DEVICE_COUNT:
        raise RuntimeError("Exactly five certificate entries are required.")

    return manifest


def publish_for_device(
    endpoint: str,
    device: dict[str, Any],
    device_index: int,
    messages_per_device: int,
    interval_seconds: float,
    start_barrier: threading.Barrier,
) -> int:
    device_id = str(device["device_id"])

    certificate_path = Path(str(device["certificate_path"]))
    private_key_path = Path(str(device["private_key_path"]))

    if not certificate_path.is_absolute():
        certificate_path = SCRIPT_DIR / certificate_path

    if not private_key_path.is_absolute():
        private_key_path = SCRIPT_DIR / private_key_path

    if not certificate_path.is_file():
        raise FileNotFoundError(
            f"Certificate file not found for {device_id}: {certificate_path}"
        )

    if not private_key_path.is_file():
        raise FileNotFoundError(
            f"Private-key file not found for {device_id}: {private_key_path}"
        )

    if not ROOT_CA_PATH.is_file():
        raise FileNotFoundError(
            f"AWS root CA file not found: {ROOT_CA_PATH}"
        )

    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=str(certificate_path),
        pri_key_filepath=str(private_key_path),
        ca_filepath=str(ROOT_CA_PATH),
        client_id=device_id,
        clean_session=True,
        keep_alive_secs=30,
        port=8883,
    )

    connection.connect().result(timeout=30)

    published_count = 0

    try:
        start_barrier.wait(timeout=30)

        for sequence in range(messages_per_device):
            event_payload = {
                "device_id": device_id,
                "latitude": round(
                    10.0 + device_index + (sequence * 0.001),
                    6,
                ),
                "longitude": round(
                    20.0 + device_index + (sequence * 0.001),
                    6,
                ),
                "aqi": 40 + (device_index * 10) + sequence,
                "timestamp": datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds",
                ),
            }

            payload = {
                "schema": {
                    "type": "struct",
                    "name": "hackathon.iot_event",
                    "optional": False,
                    "fields": [
                        {
                            "field": "device_id",
                            "type": "string",
                            "optional": False,
                        },
                        {
                            "field": "latitude",
                            "type": "double",
                            "optional": False,
                        },
                        {
                            "field": "longitude",
                            "type": "double",
                            "optional": False,
                        },
                        {
                            "field": "aqi",
                            "type": "int32",
                            "optional": False,
                        },
                        {
                            "field": "timestamp",
                            "type": "string",
                            "optional": False,
                        },
                    ],
                },
                "payload": event_payload,
            }

            publish_result = connection.publish(
                topic=MQTT_TOPIC,
                payload=json.dumps(payload),
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )

            if isinstance(publish_result, tuple):
                publish_future = publish_result[0]
            else:
                publish_future = publish_result

            publish_future.result(timeout=30)
            published_count += 1

            if sequence + 1 < messages_per_device:
                time.sleep(interval_seconds)
    finally:
        connection.disconnect().result(timeout=30)

    print(f"DEVICE_PUBLISH={device_id}:{published_count}")
    return published_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--messages-per-device", type=int, default=3)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    arguments = parser.parse_args()

    if arguments.messages_per_device < 1:
        raise ValueError("messages-per-device must be at least one.")

    manifest = load_manifest()
    start_barrier = threading.Barrier(DEVICE_COUNT)

    with ThreadPoolExecutor(max_workers=DEVICE_COUNT) as executor:
        futures = []

        for device_index, device in enumerate(manifest, start=1):
            future = executor.submit(
                publish_for_device,
                arguments.endpoint,
                device,
                device_index,
                arguments.messages_per_device,
                arguments.interval_seconds,
                start_barrier,
            )
            futures.append(future)

        results = [future.result() for future in futures]

    expected_total = DEVICE_COUNT * arguments.messages_per_device
    actual_total = sum(results)

    if actual_total != expected_total:
        raise RuntimeError(
            f"Expected {expected_total} messages but published {actual_total}."
        )

    print(f"SIMULATED_DEVICE_COUNT={DEVICE_COUNT}")
    print(f"MESSAGES_PER_DEVICE={arguments.messages_per_device}")
    print(f"TOTAL_MESSAGES={actual_total}")
    print("PARALLEL_EXECUTION=PASS")
    print("IOT_CORE_MQTT_PUBLISH=PASS")


if __name__ == "__main__":
    main()