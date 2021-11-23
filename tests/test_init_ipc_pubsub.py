import sys
import json
import pytest
import logging
import awsiot.greengrasscoreipc.model as model

# Import the src directory
sys.path.append('src')

from src.ipc_pubsub.ipc_topic_pubsub import IpcTopicPubSub

def test_init_ipc_pubsub(mocker, monkeypatch, caplog):

    """ Positove and negatove unit tests for IPC PubSub wrapper """

    caplog.set_level(logging.INFO)

    monkeypatch.setenv("AWS_IOT_THING_NAME", "TestDevice")
    ipc_connect = mocker.patch("awsiot.greengrasscoreipc.connect")
    thread_executor = mocker.patch("concurrent.futures.ThreadPoolExecutor" )

    # Read in the GG Component recipe config details
    f = open('src/recipe.json',)
    data = json.load(f)
    ggv2_component_config = data["ComponentConfiguration"]["DefaultConfiguration"]["GGV2ComponentConfig"]

    ######################################################
    # Test manual creation and negative test / exception of IPC PubSub wrapper
    #

    # Test successful init of PubSub Class
    caplog.clear()
    ipc_pubsub_timeout = ggv2_component_config['ipc_pubsub_timeout']
    ipc_subscribe_topics = ["ble_connect_topic", "ble_disconnect_topic", "ble_list_topic", "ble_scan_topic"]
    ipc_pubsub = IpcTopicPubSub(None, ipc_subscribe_topics, ipc_pubsub_timeout)

    for record in caplog.records:
        assert record.levelname != "ERROR"

    # Manual publish message tests
    caplog.clear()
    message_object = {
        "status": 200,
        "data": {
            "ble-mac": "XX:XX:XX:XX:XX:XX",
            "disconnect-status": "success"
        }
    }

    ipc_pub_topics = ["ble_connect_response_topic", "ble_disconnect_response_topic", "ble_list_response_topic", "ble_scan_response_topic", "pubsub_error_topic"]
    for ipc_pub_topic in ipc_pub_topics:
        ipc_pubsub.publish_to_topic(ipc_pub_topic, message_object)

    for record in caplog.records:
        assert record.levelname != "ERROR"

    # Manual test of Subscribing to a topic.
    event = model.IoTCoreMessage(
        message=model.MQTTMessage(
            topic_name=None, payload=json.dumps({"test": "test"}).encode()
        )
    )

    topic_subscriber = ipc_pubsub.TopicSubscriber(None, ipc_subscribe_topics, thread_executor)
    topic_subscriber.on_stream_event(event)
    topic_subscriber.on_stream_error(Exception('Test Exception'))
    topic_subscriber.on_stream_closed()
