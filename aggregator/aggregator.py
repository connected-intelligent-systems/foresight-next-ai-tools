from quixstreams import Application, message_context
import os
import json
from datetime import datetime, timezone


def convert_timestamp(timestamp_ms: int) -> str:
    """
    Convert a timestamp in milliseconds to an ISO 8601 formatted string.

    Args:
        timestamp_ms (int): The timestamp in milliseconds.

    Returns:
        str: The ISO 8601 formatted string representation of the timestamp.
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()


def custom_ts_extractor(
    value,
    headers,
    timestamp,
    timestamp_type,
) -> int:
    """
    Extracts the timestamp from Kafka message headers to get the event time.
    Args:
        value: The value of the Kafka message.
        headers: The headers of the Kafka message, expected to be a list of tuples.
        timestamp: The timestamp of the Kafka message.
        timestamp_type: The type of the timestamp.
    Returns:
        int: The extracted timestamp as an integer.
    """
    return int(dict(headers)['tb_msg_md_ts'])


def initializer(value: dict) -> dict:
    """
    Initializes and formats a dictionary for aggregation.

    Args:
        value (dict): A dictionary containing the data to be aggregated. 
                      It must include the keys 'aggregationProperty', 'ts', and 'nilmDisaggregation'.

    Returns:
        dict: A dictionary with the following structure:
                          "value": <value of the aggregationProperty key>,
                          "time": <converted timestamp>
                  "nilmDisaggregation": <value of the nilmDisaggregation key>
    """
    aggregationProperty = value.get('aggregationProperty')
    return {
        "values": [
            {
                "value": value[aggregationProperty],
                "time": convert_timestamp(value["ts"])
            }
        ],
        "nilmDisaggregation": value['nilmDisaggregation']
    }


def reducer(aggregated: dict, value: dict) -> dict:
    """
    Reduces the given value into the aggregated dictionary by appending the 
    value's power and converted timestamp to the 'values' list and updating 
    the 'nilmDisaggregation' field.

    Args:
        aggregated (dict): The dictionary containing the aggregated data.
        value (dict): The dictionary containing the new value to be added.

    Returns:
        dict: The updated aggregated dictionary with the new value appended 
              to the 'values' list and the 'nilmDisaggregation' field updated.
    """
    return {
        "values": aggregated["values"] + [{
            "value": value["power"],
            "time": convert_timestamp(value["ts"])
        }],
        "nilmDisaggregation": value['nilmDisaggregation']
    }


def filter_nilmDisaggregation(x):
    """
    Filters NILM disaggregation settings.

    This function checks if the given dictionary `x` contains the keys 'duration' and 'step' 
    with values matching the global variables `duration_ms` and `step_ms`, respectively.

    Args:
        x (dict): The dictionary to be checked for NILM disaggregation settings.

    Returns:
        bool: True if the dictionary contains the required keys with matching values, False otherwise.
    """
    return 'duration' in x and x['duration'] == duration_ms and 'step' in x and x['step'] == step_ms


def prepare_message(row: dict):
    """
    Prepares the message and extracts information from the headers.

    This function processes a given row dictionary by extracting and decoding
    specific headers from the message context. It updates the row dictionary
    with the extracted information.

    Args:
        row (dict): The dictionary representing a row of data to be processed.

    Returns:
        dict: The updated row dictionary with extracted and processed header information.

    Raises:
        json.JSONDecodeError: If there is an error decoding the JSON for the 'nilmDisaggregation' key.

    Header Keys:
        - 'tb_msg_md_ss_nilmDisaggregation': JSON encoded string that will be decoded and filtered.
        - 'tb_msg_md_ss_aggregationProperty': String representing the aggregation property.
        - 'tb_msg_md_ts': Integer timestamp.
    """
    headers = dict(message_context().headers)

    nilm_disaggregation_key = 'tb_msg_md_ss_nilmDisaggregation'
    if nilm_disaggregation_key in headers:
        try:
            row['nilmDisaggregation'] = json.loads(
                headers[nilm_disaggregation_key].decode())
            row['nilmDisaggregation'] = list(
                filter(filter_nilmDisaggregation, row['nilmDisaggregation']))
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for key {
                  nilm_disaggregation_key}: {e}")

    aggregation_property_key = 'tb_msg_md_ss_aggregationProperty'
    if aggregation_property_key in headers:
        row['aggregationProperty'] = headers[aggregation_property_key].decode()
    else:
        row['aggregationProperty'] = 'power'

    timestamp_key = 'tb_msg_md_ts'
    if timestamp_key in headers:
        row['ts'] = int(headers[timestamp_key])

    return row


def filter_message(row: dict) -> bool:
    """
    Filters a message based on specific properties.

    Args:
        row (dict): A dictionary representing a message with various properties.

    Returns:
        bool: True if the message meets the filtering criteria, False otherwise.
            The criteria are:
            - The 'nilmDisaggregation' key must be present and truthy.
            - The 'aggregationProperty' key must be present and truthy.
            - The value of the key specified by 'aggregationProperty' must be truthy.
            - The 'ts' key must be present and truthy.
    """
    aggregationProperty = row.get('aggregationProperty')
    return bool(row.get('nilmDisaggregation')) and bool(aggregationProperty) and bool(row[aggregationProperty]) and bool(row.get('ts'))


def handle_window(row: dict):
    """
    Processes a row of data and generates a list of dictionaries for each configured NILM (Non-Intrusive Load Monitoring) service.

    Args:
        row (dict): A dictionary containing the following keys:
            - 'start' (any): The start time of the window.
            - 'end' (any): The end time of the window.
            - 'value' (dict): A dictionary containing:
                - 'values' (any): The data values for the window.
                - 'nilmDisaggregation' (list): A list of dictionaries, each representing a NILM service configuration with keys:
                    - 'url' (str): The URL of the NILM service.
                    - 'duration' (any): The duration of the disaggregation.
                    - 'step' (any): The step size of the disaggregation.

    Returns:
        list: A list of dictionaries, each containing:
            - 'url' (str): The URL of the NILM service.
            - 'start' (any): The start time of the window.
            - 'end' (any): The end time of the window.
            - 'duration' (any): The duration of the disaggregation.
            - 'step' (any): The step size of the disaggregation.
            - 'data' (any): The data values for the window.
    """
    return [
        {
            'url': disaggregation['url'],
            'start': row['start'],
            'end': row['end'],
            'duration': disaggregation['duration'],
            'step': disaggregation['step'],
            'data': row['value']['values']
        }
        for disaggregation in row['value']['nilmDisaggregation']
    ]


broker_address = os.environ.get("KAFKA_BROKER_ADDRESS", "localhost:9094")
consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "asdads")
auto_offset_reset = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")
input_topic_name = os.environ.get("KAFKA_INPUT_TOPIC", "power-data")
output_topic_name = os.environ.get("KAFKA_OUTPUT_TOPIC", "output")
duration_ms = int(os.environ.get("WINDOW_DURATION_MS", "3840000"))
step_ms = int(os.environ.get("WINDOW_STEP_MS", "1200000"))

app = Application(
    broker_address=broker_address,
    consumer_group=consumer_group,
    auto_offset_reset=auto_offset_reset,
    auto_create_topics=True,
)

input_topic = app.topic(
    input_topic_name, timestamp_extractor=custom_ts_extractor)
output_topic = app.topic(output_topic_name, value_serializer='json')

sdf = app.dataframe(input_topic)
sdf = sdf.apply(prepare_message)
sdf = sdf.filter(filter_message)
sdf = sdf.hopping_window(duration_ms=duration_ms, step_ms=step_ms)
sdf = sdf.reduce(reducer=reducer, initializer=initializer)
sdf = sdf.final()
sdf = sdf.apply(handle_window, expand=True)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)
