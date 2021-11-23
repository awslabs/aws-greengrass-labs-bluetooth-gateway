import sys
import json
import pytest
import logging

# Import the src directory
sys.path.append('src')

from src.main import AwsGreengrassV2BleComponent

def test_publish_message_types(mocker, monkeypatch, caplog):
    

    """ Test all Supported PubSub Publish Message Topics """
    

    monkeypatch.setenv("AWS_IOT_THING_NAME", "TestDevice")
    ipc_connect = mocker.patch("awsiot.greengrasscoreipc.connect")

    # Read in the GG Component recipe config details
    f = open('src/recipe.json',)
    data = json.load(f)
    ggv2_component_config = data["ComponentConfiguration"]["DefaultConfiguration"]["GGV2ComponentConfig"]

    # Create / initilise the GG Component Class
    ggv2_component = AwsGreengrassV2BleComponent(ggv2_component_config)

    #Assert no ERROR logs seen during AwsGreengrassV2BleComponent initilisation
    for record in caplog.records:
        assert record.levelname != "ERROR"

    ######################################################
    # Test IPC / MQTT Publish messages

    ### TEST BLE CONTROL TOPICS

    connect_ble_topic = ggv2_component.ble_control_sub_topics[0]
    disconnect_ble_topic = ggv2_component.ble_control_sub_topics[1]
    dis_connect_message = { "ble-mac" : "3c:61:05:12:ee:0a"}

    list_ble_topic = ggv2_component.ble_control_sub_topics[2]
    scan_ble_topic = ggv2_component.ble_control_sub_topics[2]

    list_scan_message = {}

    # Publish BLE Control messages to IPC Message Bus
    caplog.clear()

    sdk = 'ipc'
    ggv2_component.publish_message(sdk, dis_connect_message, connect_ble_topic, None)
    ggv2_component.publish_message(sdk, dis_connect_message, disconnect_ble_topic, None)
    ggv2_component.publish_message(sdk, list_scan_message, list_ble_topic, None)
    ggv2_component.publish_message(sdk, scan_ble_topic, scan_ble_topic, None)

    # validate no ERRORs in sending well formatted messages
    for record in caplog.records:
        assert record.levelname != "ERROR"

    # Publish BLE Control messages to MQTT Message Bus
    caplog.clear()

    sdk = 'mqtt'
    ggv2_component.publish_message(sdk, dis_connect_message, connect_ble_topic, None)
    ggv2_component.publish_message(sdk, dis_connect_message, disconnect_ble_topic, None)
    ggv2_component.publish_message(sdk, list_scan_message, list_ble_topic, None)
    ggv2_component.publish_message(sdk, scan_ble_topic, list_ble_topic, None)

    # validate no ERRORs in sending well formatted messages
    for record in caplog.records:
        assert record.levelname != "ERROR"

    ## Publish topics
    #pubsub_control_topic = ggv2_component.pubsub_control_topic
    #pubsub_data_topic = ggv2_component.pubsub_data_topic
    #pubsub_data_rx_topic = ggv2_component.pubsub_data_rx_topic
    #pubsub_data_tx_topic = ggv2_component.pubsub_data_tx_topic
    #ble_connect_topic = ggv2_component.ble_connect_topic


    # Tes Publish to BLE Control Topics Bus (Will ERROR as no real BLE Devices connevcted)
    caplog.clear()

    ggv2_component.publish_message("ble", {}, None, "XX:XX:XX:XX:XX:XX")
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # Validate ERROR is logged sending Unknown API Type Message
    caplog.clear()
    response_messsage = ggv2_component.publish_message("unknown", {}, list_ble_topic, None)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"
