'''
main.py:

AWS Greengrass BlueTooth Low Energy (BLE) device gateway proxying messages between 
connected BLE devices and IPC / MQTT PubSub message bus.

'''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import os
import re
import sys
import json
import time
import logging
from datetime import datetime

from ipc_pubsub.ipc_topic_pubsub import IpcTopicPubSub
from ipc_pubsub.mqtt_core_pubsub import MqttCorePubSub
from ble.ble_scanner import BleScanner
from ble.ble_uart_controller import BleUartController

# Config the logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

class AwsGreengrassV2BleComponent():

    def __init__(self, ggv2_component_config):
        '''
            Initialises the AWS Greengrass V2 custom component including the IPC and MQTT PubSub Client
            Greengrass Config expected to be passed from AWS Greengrass V2 deployment recipe.
        '''
        
        try:
            #######################################################
            # Log the start of the process
            log.info('Initialising AWS Greengrass V2 BLE Gateway Component')

            super().__init__()

            #######################################################
            # Parse config from AWS Greengrass component recipe
            log.info('Parsing AWS Greengrass V2 Config.')
            log.info('AWS Greengrass V2 Config: {}'.format(ggv2_component_config))

            self.ipc_pubsub_timeout = ggv2_component_config['ipc_pubsub_timeout']
            self.mqtt_pubsub_timeout = ggv2_component_config['mqtt_pubsub_timeout']

            # Completed processing recipe driven config for the Greengrass application.
            log.info('Parsing AWS Greengrass V2 Config Complete.')

            #######################################################
            # Initilise BLE control and data MQTT/IPC topics
            log.info('Initilising BLE control MQTT/IPC topics.')
            self.thing_name  = USER = os.getenv('AWS_IOT_THING_NAME')
            self.pubsub_thing_topic = 'aws-greengrass/things/{}/ble'.format(self.thing_name)

            self.pubsub_control_topic = '{}/control'.format(self.pubsub_thing_topic)
            self.pubsub_data_topic = '{}/data'.format(self.pubsub_thing_topic)
            self.pubsub_ble_state_topic = '{}/state'.format(self.pubsub_thing_topic)
            self.pubsub_error_topic = '{}/error'.format(self.pubsub_thing_topic)

            # Rx / TX topics for proxying BLE <-> IPC / MQTT. 
            # Uses cloud perspective. i.e: TX is cloud >-tx-> to BLE. RX is cloud <-rx-< from BLE
            self.pubsub_data_rx_topic = '{}/rx'.format(self.pubsub_data_topic)
            self.pubsub_data_tx_topic = '{}/tx'.format(self.pubsub_data_topic)

            # For each supported control message, create a /command and command/response topic
            self.ble_connect_topic = '{}/connect'.format(self.pubsub_control_topic)
            self.ble_connect_response_topic = '{}/response'.format(self.ble_connect_topic)

            self.ble_disconnect_topic = '{}/disconnect'.format(self.pubsub_control_topic)
            self.ble_disconnect_response_topic = '{}/response'.format(self.ble_disconnect_topic)

            self.ble_list_topic = '{}/list'.format(self.pubsub_control_topic)
            self.ble_list_response_topic = '{}/response'.format(self.ble_list_topic)

            self.ble_scan_topic = '{}/scan'.format(self.pubsub_control_topic)
            self.ble_scan_response_topic = '{}/response'.format(self.ble_scan_topic)


            self.ble_control_pub_topics = [self.ble_connect_response_topic, self.ble_disconnect_response_topic, self.ble_list_response_topic, self.ble_scan_response_topic, self.pubsub_ble_state_topic, self.pubsub_error_topic]
            self.ble_control_sub_topics = [self.ble_connect_topic, self.ble_disconnect_topic, self.ble_list_topic, self.ble_scan_topic]

            log.info('BLE Control Publish Topics: {}'.format(self.ble_control_pub_topics))
            log.info('BLE Control Subscribe Topics: {}'.format(self.ble_control_sub_topics))

            log.info('Initilising BLE control MQTT/IPC topics complete.')

            #######################################################
            # Initilise internal BLE proxy topic to pass BLE messages into the message routing / processing methods
            # Note: Don't expose this externally, is ony for internal BLE to PubSub proxy identification.
            self.ble_proxy_topic = '$aws/greengrass/ble/do-proxy/'

            #######################################################
            # Init local Topic and MQTT IoT Core PubSub message service.

            log.info('Initialising IPC Topic PubSub inter-service messaging.')
            self.ipc_topic_pubsub = IpcTopicPubSub(self.receive_message_router, self.ble_control_sub_topics, self.ipc_pubsub_timeout)

            log.info('Initialising IPC MQTT IoT Core PubSub messaging.')
            self.mqtt_core_pubsub = MqttCorePubSub(self.receive_message_router, self.ble_control_sub_topics, self.mqtt_pubsub_timeout)

            #######################################################
            # Init the Bluetooth Low Energy (BLE) message controller
            log.info('Initialising Bluetooth BLE Discovery and Control service')
            self.ble_scanner = BleScanner()
            self.ble_controller = BleUartController(self.receive_message_router, self.ble_state_change_callback)
            log.info('Initialising Bluetooth BLE Discovery and Control service Complete')

            log.info('Initialising AWS Greengrass V2 BLE Gateway Component complete!')

        except ValueError as val_error:  # pragma: no cover  # includes JSON parsing errors
            msg = 'VAL_ERROR: {} - GREENGRASS CONFIG: {}'.format(val_error, ggv2_component_config)
            log.error(msg)
            self.publish_error(500, msg)

        except KeyError as key_error:  # pragma: no cover  # includes requests for fields that don't exist in the received config
            msg = 'KEY_ERROR: {} - GREENGRASS CONFIG: {}'.format(key_error, ggv2_component_config)
            log.error(msg)
            self.publish_error(500, msg)

        except Exception as err:  # pragma: no cover 
            msg = 'EXCEPTION: {} - GREENGRASS CONFIG: {}'.format(err, ggv2_component_config)
            log.error(msg)
            self.publish_error(500, msg)
    
    ##################################################
    ### PubSub Message Callback
    ##################################################

    def receive_message_router(self, topic, payload):
        '''
        Receive message handler / router for all (IPC and MQTT) PubSub and BLE messages. 
        Validates and routes messages based on topic. PubSub messages read in from 
        IPC and MQTT message bus with subscribed topic presented for routing and processing. 

        Internally, messages received from BLE devices are given the the topic: $aws/greengrass/ble/do-proxy/BLE_MAC
        This uses the '$aws/' reserved topic format but is never exposed externally to the IPC or MQTT message bus.

        Any BLE message received on this topic will be re-published / proxied on IPC and MQTT (depending on config settings) 
        message bus on the defined data topic (default: aws/things/THING_NAME/ble/data/) that is appended with /rx/BLE_MAC
        address of the sending device: (i.e: aws/things/THING_NAME/ble/data/rx/XX:XX:XX:XX:XX)

        Using this pattern for BLE received messages can be routed and processed in the same method as PubSub 
        and normalises the message flow across both protocols. 
        '''

        try:
            # Debug received message
            log.debug('RECEIVED MESSAGE Topic: {} - Message: {}'.format(topic, payload))

            # Note: Expects all ingress messages to be in JSON format
            message_object = json.loads(payload)

            # Route BLE Control messages (i.e: connect, list, disconnect)
            if topic.startswith(self.pubsub_control_topic):
                self.pubsub_control_router(topic, message_object)

            # Process BLE messages on internal BLE proxy topic to PubSub.
            elif topic.startswith(self.ble_proxy_topic):
                self.proxy_ble_to_pubsub(topic, message_object)

            # Process PubSub messages proxy to BLE 
            elif topic.startswith(self.pubsub_data_tx_topic):
                self.proxy_pubsub_to_ble(topic, message_object)

            else:
                raise Exception('Received mesage on unknown / unsupported topic.')

        except Exception as err:
            msg = 'Message Callback Exception: {} - Topic {} - Payload: {}'.format(err, topic, payload)
            log.error(msg)
            self.publish_error(500, msg)

    ##################################################
    ### PubSub BLE Control Message Router and Processors
    ##################################################

    def pubsub_control_router(self, topic, message):
        '''
        Route BLE Control messages such as Connect, Disconnect amd List BLE Devices
        Topic expected format 'aws-greengrass/things/THING_NAME/ble/control/CONTROL_COMMAND' as per pubsub_control_topic
        '''

        try:
            topic_split = topic.split('/')

            if not len(topic_split) == 6:
                raise Exception('Received BLE control command with unsupported topic structure')
            
            control_command = topic_split[5]

            log.debug('Received BLE Control Message Topic: {} -  Control Command: {}, Message: {}'.format(topic, control_command, message))

            if control_command == 'connect':
                # Get the expected BLE MAC address in a connect control message
                ble_mac = message['ble-mac']
                self.connect_ble_device(ble_mac)

            elif control_command == 'disconnect':
                # Get the expected BLE MAC address in a disconnect control message
                ble_mac = message['ble-mac']
                self.disconnect_ble_device(ble_mac)

            elif control_command == 'list':
                self.get_ble_device_list()
            
            elif control_command == 'scan':
                self.scan_ble_devices()
            
            else:
                raise Exception('Received message on unknown BLE Gateway Control Topic')

        except ValueError as val_error: # includes JSON parsing errors
            msg = 'BLE Control Routing VAL_ERROR: {} - Topic {} - Message: {}'.format(val_error, topic, message)
            log.error(msg)
            self.publish_error(500, msg)

        except KeyError as key_error: # includes requests for fields that don't exist
            msg = 'BLE Control Routing KEY_ERROR: {} - Topic {} - Message: {}'.format(key_error, topic, message)
            log.error(msg)
            self.publish_error(500, msg)

        except Exception as err:
            msg = 'BLE Control Routing EXCEPTION: {} - Topic {} - Message: {}'.format(err, topic, message)
            log.error(msg)
            self.publish_error(500, msg)

    def connect_ble_device(self, ble_mac):

        try:

            self.ble_controller.connect_ble_device(ble_mac)

            # Publish message to confirm connectio nis processing to ble/control/connect/response topic
            # Actual connection will async publish any state changes to /ble/control/status
            connect_response = {
                "status": 200,
                "data": {
                    "ble-mac": ble_mac,
                    "connect_status": "request-accepted"
                }
            }

            # Publish response to IPC and MQTT
            self.publish_message('ipc', connect_response, topic=self.ble_connect_response_topic)
            self.publish_message('mqtt', connect_response, topic=self.ble_connect_response_topic)

        except Exception as err:
            msg = 'BLE Device: {} - Connect Error: {}'.format(ble_mac, err)
            log.error(msg)
            self.publish_error(500, msg)

    def disconnect_ble_device(self, ble_mac):

        try:

            self.ble_controller.disconnect_ble_device(ble_mac)

            # Publish message to confirm disconnection attempt is processing to ble/control/disconnect/response topic
            # Actual connection status will async publish any state changes to /ble/control/status
            disconnect_response = {
                "status": 200,
                "data": {
                    "ble-mac": ble_mac,
                    "disconnect_status": "request-accepted"
                }
            }

            # Publish response to IPC and MQTT
            self.publish_message('ipc', disconnect_response, topic=self.ble_disconnect_response_topic)
            self.publish_message('mqtt', disconnect_response, topic=self.ble_disconnect_response_topic)

        except Exception as err:
            msg = 'BLE Device: {} - Disconnect Error: {}'.format(ble_mac, err)
            log.error(msg)
            self.publish_error(500, msg)
    
    def get_ble_device_list(self):

        try:

            log.info('Device List Requested')
            device_list_response = self.ble_controller.get_active_ble_devices()
            log.info('Device List Response: {}'.format(device_list_response))

            # Publish response to IPC and MQTT
            self.publish_message('ipc', device_list_response, topic=self.ble_list_response_topic)
            self.publish_message('mqtt', device_list_response, topic=self.ble_list_response_topic)

        except Exception as err:
            msg = 'BLE Device List Request Error: {}'.format(err)
            log.error(msg)
            self.publish_error(500, msg)

    def scan_ble_devices(self):

        try:

            log.info('BLE Scan Requested')
            device_scan_response = self.ble_scanner.scan_ble_devices(5)
            log.info('Ble Scan Response: {}'.format(device_scan_response))

            # Publish respnse to IPC and MQTT
            self.publish_message('ipc', device_scan_response, topic=self.ble_scan_response_topic)
            self.publish_message('mqtt', device_scan_response, topic=self.ble_scan_response_topic)

        except Exception as err:
            msg = 'BLE Device Scan Request Error: {}'.format(err)
            log.error(msg)
            self.publish_error(500, msg)

    ##################################################
    ### PubSub <-> BLE Message Proxys
    ##################################################

    def proxy_pubsub_to_ble(self, topic, message_object):
        '''
        Proxy PubSub message to the BLE Device.

        Topic expected format: aws/things/THING_NAME/ble/data/tx/BLE_MAC as per pubsub_data_tx_topic
        Extracts the BLE MAC address from the topic and forwards the message over BLE.

        '''

        try:
            # Get the target device BLE Mac address and proxy PubSub message over BLE to device.
            ble_mac = self.get_validate_topic_mac(topic, 6)

            # TODO: Move log info to log debug.
            log.info('Proxying PubSub message to BLE: BLE_MAC: {} - TOPIC: {} - MESSAGE: {}'.format(ble_mac, topic, message_object))

            # Proxy / Publish message to BLE device. 
            self.ble_controller.publish_to_ble(ble_mac, message_object)

        except Exception as err:
            msg = 'PubSub to BLE Proxy ERROR: {} - TOPIC {} - PAYLOAD: {}'.format(err, topic, message_object)
            log.error(msg)
            self.publish_error(500, msg)
    
    def proxy_ble_to_pubsub(self, topic, message_object):
        '''
        Proxy BLE message to PubSub IPC / MQTT message bus

        Topic expected format: '$aws/greengrass/ble/do-proxy/BLE_MAC' as per ble_proxy_topic
        Extracts the BLE MAC address from the topic and forwards the message 
        to PubSub aws/things/THING_NAME/ble/data/rx/BLE_MAC 
        '''

        try:
            # Get the target device BLE Mac address and proxy PubSub message over BLE to device.
            ble_mac = self.get_validate_topic_mac(topic, 4)
            # TODO: Move log info to log debug.
            log.info('Proxying BLE message to PubSub: BLE_MAC: {} - TOPIC: {} - MESSAGE: {}'.format(ble_mac, topic, message_object))

            # Proxy message to PubSub message bus. 
            topic = '{}/{}'.format(self.pubsub_data_rx_topic, ble_mac)
            self.publish_message('ipc', message_object, topic=topic)
            self.publish_message('mqtt', message_object, topic=topic)

        except Exception as err:
            msg = 'PubSub to BLE Proxy ERROR: {} - TOPIC {} - PAYLOAD: {}'.format(err, topic, message_object)
            log.error(msg)
            self.publish_error(500, msg)

    def get_validate_topic_mac(self, topic, element_number):
        '''
        A simple helper to extract and validate MAC adresses from PubSub topics.
        '''
        topic_split = topic.split('/')

        # Validate requested element exists
        if len(topic_split) <= element_number:
            raise Exception('Can\'t extract device MAC from topic request at element: {}.'.format(element_number))
        
        # Get BLE Mac address and normalise to uppercase
        topic_mac = topic_split[element_number].upper()

        # Validate Mac address with RegEx
        if not re.match('([0-9A-F]{2}:){5}([0-9A-F]{2})', topic_mac):
            raise Exception('{} is not a valid MAC address.'.format(topic_mac))

        # If all validation passed, return MAC address taken from topic element requested.
        return topic_mac

    ##################################################
    ### BLE State Change / Control Message Callbacks
    ##################################################
    def ble_state_change_callback(self, ble_mac, current_state, previous_state):

        try:
            # If transitioning to a conected state, subscribe to the BLE devices TX IPC / MQTT topic.
            # if this connection has bounced, the subscribe function will gracefully ignore duplicate subscriptions.
            if self.ble_controller.is_peripheral_connected(ble_mac):
                log.info('BLE MAc: {} State Change to connected, subscribing to TX Topics.'.format(ble_mac))
                ble_tx_topic = '{}/{}'.format(self.pubsub_data_tx_topic, ble_mac)
                self.ipc_topic_pubsub.subscribe_to_topic(ble_tx_topic)
                self.mqtt_core_pubsub.subscribe_to_topic(ble_tx_topic)

            status_update = {
                "control-command" : "ble-conection-state-changed",
                "ble-mac" : ble_mac, 
                "updated" : datetime.now().strftime("%Y%m%d%H%M%S%f"),
                "data" : { 
                    "previous-state" : previous_state,
                    "current_state" : current_state,
                }
            }

            self.publish_message('ipc', status_update, topic=self.pubsub_ble_state_topic)
            self.publish_message('mqtt', status_update, topic=self.pubsub_ble_state_topic)

        except Exception as err:
            msg = 'BLE Mac: {} State Change ERROR: {}'.format(ble_mac, err)
            log.error(msg)
            self.publish_error(500, msg)

    ##################################################
    ### PubSub / BLE Message Publisher
    ##################################################

    def publish_message(self, publish_api, message_object, topic=None, ble_mac=None):
        '''
            IPC / MQTT / BLE message publisher.
            publish_api: ['ipc' | 'mqtt' | 'ble']
            topic: IPC or MQTT Topic, ignored if publish_api == 'ble'
            ble_mac: MAC adress to publish BLE message, ignored if publish_api == 'ipc' or 'mqtt'
        '''

        try:
            # Publish the message to the AWS Greengrass IPC or MQTT APIs
            if publish_api == 'ipc':
                self.ipc_topic_pubsub.publish_to_topic(topic, message_object)

            elif publish_api == 'mqtt':
                self.mqtt_core_pubsub.publish_to_mqtt(topic, message_object)

            elif publish_api == 'ble':
                self.ble_controller.publish_to_ble(ble_mac, message_object)

            else:
                raise Exception('Publish for unknown publish_api: {}'.format(publish_api))

        except Exception as err:
            msg = 'EXCEPTION: Error Publishing PUBLISH_API: {} - TOPIC: {} - MESSAGE: {} - ERROR: {}'.format(publish_api, topic, message_object, err)
            log.error(msg)
            self.publish_error(500, msg)
    
    ##################################################
    ### PubSub / BLE Error Publisher
    ##################################################

    def publish_error(self, error_code, error_msg):
        '''
        Publish error_msg to IPC and MQTT.
        '''

        message_object = {
            "status" : error_code,
            "error-message" : error_msg
        }
        self.publish_message('ipc', message_object, topic=self.pubsub_error_topic)
        self.publish_message('mqtt', message_object, topic=self.pubsub_error_topic)

    ##################################################
    # Main service / process application logic
    ##################################################
    
    def service_loop(self):
        '''
        Holds the  component process up while driven by BLE / IPC / MQTT 
        Event driven PubSub logic 
        '''

        # Lopp delay to hold process open.
        while True:
            time.sleep(5)

if __name__ == "__main__":

    try:
        # Accepts the Greengrass V2 config from deployment recipe into sys.argv[1] as shown in:
        # [AWS Samples GIT Repo TBA]
        ggv2_component_config = json.loads(sys.argv[1])
        ggv2_component = AwsGreengrassV2BleComponent(ggv2_component_config)

        # The main process loop, add application logic in this function.
        ggv2_component.service_loop()

    except Exception as err:
        log.error('EXCEPTION: Exception occurred initialising component. ERROR MESSAGE: {}'.format(err))
