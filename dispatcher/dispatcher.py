from quixstreams import Application, message_context
from datetime import datetime
import os
import requests
import paho.mqtt.client as mqtt
import certifi
import json
import uuid


def generate_unique_id(device_id: str, url: str) -> str:
    """
    Generates a unique identifier based on the provided device ID and URL.

    This function uses the UUID version 5 algorithm, which generates a UUID
    based on a namespace and a name. The namespace is a global variable, and
    the name is a combination of the device ID and URL.

    Args:
        device_id (str): The unique identifier for the device.
        url (str): The URL associated with the device.

    Returns:
        str: A unique identifier string generated from the device ID and URL.
    """
    global namespace
    return str(uuid.uuid5(namespace, f"{device_id}-{url}"))


def send_connect(id: str):
    """
    Sends a connect message to ThingsBoard via MQTT gateway.

    Args:
        id (str): The device identifier to be included in the connect message.

    Publishes a JSON payload to the "v1/gateway/connect" topic with the device ID and type set to "power".
    """
    global mqtt_client
    mqtt_client.publish("v1/gateway/connect", json.dumps({
        "device": id,
        "type": "power"
    }))


def send_attribute(id: str, device_id: str, url: str):
    """
    Sends attributes to ThingsBoard MQTT gateway.

    Args:
        id (str): The identifier for the attribute.
        device_id (str): The ID of the device from which the attribute is disaggregated.
        url (str): The URL indicating the source of disaggregation.

    Returns:
        None
    """
    global mqtt_client
    mqtt_client.publish("v1/gateway/attributes", json.dumps({
        id: {
            "disaggregatedFrom": device_id,
            "disaggregatedBy": url
        }
    }))


def send_telemetry(id: str, results: list):
    """
    Send telemetry data to ThingsBoard MQTT gateway.

    Args:
        id (str): The identifier for the telemetry data.
        results (list): A list of dictionaries containing telemetry data. Each dictionary should have
                        'time' and 'value' keys, where 'time' is an ISO formatted timestamp and 'value'
                        is the power measurement.

    Returns:
        None
    """
    global mqtt_client
    mqtt_client.publish("v1/gateway/telemetry", json.dumps({
        id: [
            {
                "ts": int(datetime.fromisoformat(result['time']).timestamp() * 1000),
                "values": {
                    "power": float(result['value'])
                }
            }
            for result in results
        ]
    }))


def dispatch_message(row: dict):
    """
    Handles an incoming message and dispatches it to the corresponding NILM service.

    Args:
        row (dict): A dictionary containing the URL and data to be sent to the NILM service.
            - 'url' (str): The URL of the NILM service.
            - 'data' (dict): The data to be sent in the POST request.

    Returns:
        None

    Side Effects:
        - Sends a POST request to the specified NILM service URL with the provided data.
        - Prints the response status code from the NILM service.
        - If the response is successful, processes the response JSON and sends connect, attribute, and telemetry messages.
    """
    device_id = message_context().key.decode()
    response = requests.post(row['url'], json=row['data'])
    print(f"Response from {row['url']}: {response.status_code}")
    if response.ok:
        results = response.json()
        id = generate_unique_id(device_id, row['url'])
        send_connect(id)
        send_attribute(id, device_id, row['url'])
        send_telemetry(id, results)


namespace = uuid.UUID("799a6a67-083b-4411-a49f-c8f1acda2ff1")
broker_address = os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9094")
consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "disaggregation")
auto_offset_reset = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")
input_topic_name = os.environ.get("KAFKA_INPUT_TOPIC", "output")
mqtt_host = os.environ.get("MQTT_HOST", "localhost")
mqtt_port = os.environ.get("MQTT_PORT", 1883)
mqtt_username = os.environ.get("MQTT_USERNAME", "admin")
mqtt_password = os.environ.get("MQTT_PASSWORD", None)
mqtt_enable_tls = os.environ.get("MQTT_ENABLE_TLS", "false").lower() == "true"

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

if mqtt_enable_tls:
    mqtt_client.tls_set(certifi.where())
mqtt_client.username_pw_set(mqtt_username, mqtt_password)
mqtt_client.connect(mqtt_host, int(mqtt_port), 60)

app = Application(
    broker_address=broker_address,
    consumer_group=consumer_group,
    auto_offset_reset=auto_offset_reset,
    auto_create_topics=True,
)

input_topic = app.topic(input_topic_name)

sdf = app.dataframe(input_topic)
sdf = sdf.update(dispatch_message)

if __name__ == "__main__":
    mqtt_client.loop_start()
    app.run(sdf)
    mqtt_client.loop_stop()
