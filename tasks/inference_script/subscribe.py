# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
import json
from utils.command_line_utils import CommandLineUtils

# This sample uses the Message Broker for AWS IoT to send and receive messages
# through an MQTT connection. On startup, the device connects to the server,
# subscribes to a topic, and begins publishing messages to that topic.
# The device should receive those same messages back from the message broker,
# since it is subscribed to that same topic.

# cmdData is the arguments/input from the command line placed into a single struct for
# use in this sample. This handles all of the command line parsing, validating, etc.
# See the Utils/CommandLineUtils for more information.
cmdData = CommandLineUtils.parse_sample_input_pubsub()

received_count = 0
received_all_event = threading.Event()

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    try:
        # Initialize an empty list to store all messages
        message_dict = json.loads(payload)
        message_dict['date'] = int(message_dict['datetime'][:2])-1
        message_dict['month'] = int(message_dict['datetime'][2:4])
        message_dict['year'] = int('20'+message_dict['datetime'][4:])
        message_dict['time'] = "23:59:00"
        all_messages = []
        # Open the file in read mode (optional, to check for existing data)
        with open("/app/buffer/message_broker/outline.json", "r") as f:
            try:
                # Attempt to load existing data as a list (assuming the file is empty or a list)
                existing_data = json.load(f)
            except json.JSONDecodeError:  # Handle empty file or non-list data
                existing_data = []
        # Combine existing data (if any) with the new message
        all_messages.extend(existing_data)
        all_messages.append(message_dict)
        # Open the file in write mode (truncate existing content)
        with open("/app/buffer/message_broker/outline.json", "w") as myfile:
            # Write the combined list of messages as JSON to the file
            json.dump(all_messages, myfile, indent=4)
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
    global received_count
    received_count += 1

    # if received_count == 100:
    #     received_count = 0
    #     print("Received 100 messages!")
    # if received_count == cmdData.input_count:
    #     received_all_event.set()

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

if __name__ == '__main__':
    # Create the proxy options if the data is present in cmdData
    proxy_options = None
    if cmdData.input_proxy_host is not None and cmdData.input_proxy_port != 0:
        proxy_options = http.HttpProxyOptions(
            host_name=cmdData.input_proxy_host,
            port=cmdData.input_proxy_port)

    # Create a MQTT connection from the command line data
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=cmdData.input_endpoint,
        port=cmdData.input_port,
        cert_filepath=cmdData.input_cert,
        pri_key_filepath=cmdData.input_key,
        ca_filepath=cmdData.input_ca,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=cmdData.input_clientId,
        clean_session=False,
        keep_alive_secs=30,
        http_proxy_options=proxy_options,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    if not cmdData.input_is_ci:
        print(f"Connecting to {cmdData.input_endpoint} with client ID '{cmdData.input_clientId}'...")
    else:
        print("Connecting to endpoint with client ID")
    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    message_count = cmdData.input_count
    message_topic = cmdData.input_topic
    message_string = cmdData.input_message

    # Subscribe
    print("Subscribing to topic '{}'...".format(message_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=message_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))
    print("Waiting for messages to be received...")


    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
    received_all_event.wait()
    print("{} message(s) received.".format(received_count))

    # # Disconnect
    # print("Disconnecting...")
    # disconnect_future = mqtt_connection.disconnect()
    # disconnect_future.result()
    # print("Disconnected!")# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
