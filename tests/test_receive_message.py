import sys
import json
import pytest
import logging

# Import the src directory
sys.path.append('src')

from src.main import AwsGreengrassV2BleComponent

def test_receive_message_types(mocker, monkeypatch, caplog):

    """ Test all Supported PubSub Receive Message Types """

    monkeypatch.setenv("AWS_IOT_THING_NAME", "TestDevice")
    ipc_connect = mocker.patch("awsiot.greengrasscoreipc.connect")

    caplog.clear()

    # Read in the GG Component recipe config details
    f = open('src/recipe.json',)
    data = json.load(f)
    ggv2_component_config = data["ComponentConfiguration"]["DefaultConfiguration"]["GGV2ComponentConfig"]

    # Create / initilise the GG Component Class
    ggv2_component = AwsGreengrassV2BleComponent(ggv2_component_config)

    #Assert no ERROR logs seen during AwsGreengrassV2Component initilisation
    for record in caplog.records:
        assert record.levelname != "ERROR"


    ## Simulate receive control topic messages

    # Test connect - will ERROR as invalid MAC address
    caplog.clear()
    connect_topic = ggv2_component.ble_connect_topic
    dis_connect_message = json.dumps({ "ble-mac" : "XX:XX:XX:XX:XX:XX"})
    ggv2_component.receive_message_router(connect_topic, dis_connect_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # Test connect - will ERROR as devoce not present
    caplog.clear()
    connect_topic = ggv2_component.ble_connect_topic
    dis_connect_message = json.dumps({ "ble-mac" : "AA:BB:CC:DD:EE:FF"})
    ggv2_component.receive_message_router(connect_topic, dis_connect_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # Test connect with unknown extension to topic - will ERROR as device not present
    caplog.clear()
    connect_topic = '{}/unsupported/extensions'.format(ggv2_component.ble_connect_topic)
    ggv2_component.receive_message_router(connect_topic, dis_connect_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # Test disconnect
    caplog.clear()
    dis_connect_topic = ggv2_component.ble_disconnect_topic
    ggv2_component.receive_message_router(dis_connect_topic, dis_connect_message)
    for record in caplog.records:
        assert record.levelname != "ERROR"

    # Test List command
    caplog.clear()
    list_topic = ggv2_component.ble_list_topic
    list_scan_message = json.dumps({ "ble-mac" : "XX:XX:XX:XX:XX:XX"})
    ggv2_component.receive_message_router(list_topic, list_scan_message)
    for record in caplog.records:
        assert record.levelname != "ERROR"

    # Test Scan command - will ERROR as needs SUDO permissions.
    caplog.clear()
    scan_topic = ggv2_component.ble_scan_topic
    ggv2_component.receive_message_router(scan_topic, list_scan_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"


    # Test unknown control command
    caplog.clear()
    unknown_topic = '{}/unknown_command'.format(ggv2_component.pubsub_control_topic)
    ggv2_component.receive_message_router(unknown_topic, list_scan_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # # Test ble_proxy_topic
    caplog.clear()
    ble_proxy_topic = ggv2_component.ble_proxy_topic
    ble_proxy_message = json.dumps({ "command" : "toggle_led"})
    ggv2_component.receive_message_router(ble_proxy_topic, ble_proxy_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"


    # # Test pubsub_data_topic
    caplog.clear()
    pubsub_data_rx_topic = ggv2_component.pubsub_data_rx_topic
    ggv2_component.receive_message_router(pubsub_data_rx_topic, ble_proxy_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"


    caplog.clear()
    pubsub_data_tx_topic = ggv2_component.pubsub_data_tx_topic
    ggv2_component.receive_message_router(pubsub_data_tx_topic, ble_proxy_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"


    # Force Exception by receiving invalid JSON string in message payload
    caplog.clear()
    ggv2_component.receive_message_router(ble_proxy_topic,  "Im not a valid JSON string")
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # ## Force Exception by claiomimg message is from unknown topic
    caplog.clear()
    ggv2_component.receive_message_router("unknown/topic",  ble_proxy_message)
    assert caplog.records[len(caplog.records)-1].levelname == "ERROR"

    # ## Force Exception by setting Status to error value
    # caplog.clear()
    # response_messsage['response']['status'] = 500
    # json_response_messsage = json.dumps(response_messsage)
    # ggv2_component.pubsub_message_callback( ggv2_component_config['ipc_service_topic'],  json_response_messsage)
    # assert caplog.records[len(caplog.records)-1].levelname == "ERROR"
    # response_messsage['response']['status'] = 200


    # ## Force Exception by sending unknown ReqRes value
    # caplog.clear()
    # request_messsage['reqres'] = 'UnknownReqResType'
    # json_request_messsage = json.dumps(request_messsage)
    # ggv2_component.pubsub_message_callback( ggv2_component_config['ipc_service_topic'],  json_request_messsage)
    # assert caplog.records[len(caplog.records)-1].levelname == "ERROR"
