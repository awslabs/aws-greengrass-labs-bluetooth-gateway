'''
mqtt_core_pubsub.py:

Provides an AWS Greengrass V2 IPC PubSub client that manages subscriptions 
and a method to publish to AWS Greengrass MQTT topics. This is intended
for use in an AWS Greengrass V2 Component to provide PubSub services. 

IPC MQTT core is for communications between AWS Greengrass Components and 
the AWS IoT Core. This can be used to send MQTT messaging to the AWS IoT core 
to save data, trigger alarms or alerts or to trigger other AWS services and applications.

 For more detail on AWS Greengrass V2 see the Developer Guide at:
https://docs.aws.amazon.com/greengrass/v2/developerguide/what-is-iot-greengrass.html

'''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import json
import logging
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (
    PublishToIoTCoreRequest,
    SubscribeToIoTCoreRequest,
    IoTCoreMessage,
    UnauthorizedError,
    QOS
)

# Init the logger.
log = logging.getLogger(__name__)

class MqttCorePubSub():

    def __init__(self, message_callback, mqtt_subscribe_topics, mqtt_timeout=5):

        log.info('Initialising AWS Greengrass V2 MQTT IoT Core PubSub Client')

        super().__init__()

        try:

            # PubSub timeout secs. 
            self.mqtt_timeout = mqtt_timeout
            
            # PubSub message callback.
            self.message_callback = message_callback

            # MQTT Subscribe Topics
            self.mqtt_subscribe_topics = mqtt_subscribe_topics

            # Create the mqtt_clients
            self.mqtt_subscribe_client = awsiot.greengrasscoreipc.connect()
            self.mqtt_publish_client = awsiot.greengrasscoreipc.connect()

            # Init MQTT PubSub's
            self.mqtt_qos = QOS.AT_LEAST_ONCE   ## TODO - Paramaterise this into config
            self.init_mqtt_subscriber()
            self.init_mqtt_publisher()

        except InterruptedError as iErr: # pragma: no cover 
            log.exception('INTERRUPTED_EXCEPTION: MQTT Iot Core Publisher / Subscriber init was interrupted. ERROR MESSAGE: {}'.format(iErr))

        except Exception as err: # pragma: no cover 
            log.exception('EXCEPTION: Exception occurred initialising AWS Greengrass IPC MQTT Core PubSub. ERROR MESSAGE: {}'.format(err))

    ###############################################
    # IPC MQTT Iot Core PubSub Functions
    def init_mqtt_subscriber(self):
        '''
            Initialise subscription to requested MQTT IoT Core topics.
        '''
        
        self.handler = MqttCorePubSub.MqttSubscriber(self.message_callback)

        for subscribe_topic in self.mqtt_subscribe_topics:
            self.subscribe_to_topic(subscribe_topic)

    def subscribe_to_topic(self, topic):

        try:
            log.info('MQTT subscribing to topic: {}'.format(topic))
            request = SubscribeToIoTCoreRequest()
            request.topic_name = topic
            request.qos = self.mqtt_qos
            operation = self.mqtt_subscribe_client.new_subscribe_to_iot_core(self.handler)
            future = operation.activate(request)
            # call the result to ensure the future has completed.
            future.result(self.mqtt_timeout)
            log.info('Complete MQTT subscribing to topic: {}'.format(topic))

            return {'status': 200,  'data' : {'topic' : topic, 'message' : 'MQTT Subscribe to topic successful'}}

        except concurrent.futures.TimeoutError as e: # pragma: no cover 
            msg = 'TIMEOUT_ERROR: Timeout occurred while subscribing to IPC MQTT topic. ERROR MESSAGE: {} - TOPIC {}'.format(e, topic)
            log.error(msg)
            return {'status': 500,  'data' : {'topic' : topic, 'message' : msg}}

        except UnauthorizedError as e: # pragma: no cover 
            msg = 'UNATHORIZED_ERROR: Unauthorized error while subscribing to IPC MQTT topic. ERROR MESSAGE: {} - TOPIC: {}'.format(e, topic)
            log.error(msg)
            return {'status': 500,  'data' : {'topic' : topic, 'message' : msg}}

        except Exception as e: # pragma: no cover 
            msg = 'EXCEPTION: Exception while subscribing to IPC MQTT topic. ERROR MESSAGE: {} - TOPIC: {}'.format(e, topic)
            log.error(msg)
            return {'status': 500,  'data' : {'topic' : topic, 'message' : msg}}

    def init_mqtt_publisher(self):
        '''
            Initialise publisher to requested IoT Core MQTT topics.
        '''

        try:
            log.info('Initialising MQTT Publisher.')
            self.mqtt_request = PublishToIoTCoreRequest()
            self.mqtt_request.qos = self.mqtt_qos

        except Exception as err: # pragma: no cover 
            log.exception('EXCEPTION: Exception Initialising MQTT Publisher. ERROR MESSAGE: {}'.format(err))

    def publish_to_mqtt(self, topic, message_object):
        '''
            Publish a Python object serlized as a JSON message to the IoT Core MQTT topic.
        '''
        
        try:

            log.info('MQTT PUBLISH: topic: {} - Message: {}'.format(topic, message_object))
            self.mqtt_request.topic_name = topic
            json_message = json.dumps(message_object)
            self.mqtt_request.payload = bytes(json_message, "utf-8")
            operation = self.mqtt_publish_client.new_publish_to_iot_core()
            operation.activate(self.mqtt_request)
            future = operation.get_response()
            future.result(self.mqtt_timeout)

        except KeyError as key_error: # pragma: no cover  # includes requests for fields that don't exist in the received object
            log.error('KEY_ERROR: KeyError occurred while publishing to IoT Core on MQTT Topic. ERROR MESSAGE: {} - TOPIC: {} - MESSAGE: {}'.format(key_error,  topic, message_object))

        except concurrent.futures.TimeoutError as timeout_error: # pragma: no cover 
            log.error('TIMEOUT_ERROR: Timeout occurred while publishing to IoT Core on MQTT Topic. ERROR MESSAGE: {} - TOPIC: {} - MESSAGE: {}'.format(timeout_error,  topic, message_object))

        except UnauthorizedError as unauth_error: # pragma: no cover 
            log.error('UNAUTHORIZED_ERROR: Unauthorized error while publishing to IoT Core on MQTT Topic. ERROR MESSAGE: {} - TOPIC: {} - MESSAGE: {}'.format(unauth_error,  topic, message_object))

        except Exception as err: # pragma: no cover 
            log.error('EXCEPTION: Exception while publishing to IoT Core on MQTT Topic. ERROR MESSAGE: {} - TOPIC: {} - MESSAGE: {}'.format(err, topic, message_object))
    
    class MqttSubscriber(client.SubscribeToIoTCoreStreamHandler):

        def __init__(self, message_callback):

            log.info('Initialising AWS Greengrass V2 IPC MQTT Subscribe Client')

            super().__init__()

            # Create ThreadPoolExecutor to process PubSub reveived messages.
            self.executor = ThreadPoolExecutor(max_workers=None) 

            self.message_callback = message_callback

        # Topic subscription event handlers 
        def on_stream_event(self, event: IoTCoreMessage) -> None:
            try:
                
                log.info('MQTT EVENT RECEIVED: {}'.format(event))

                topic = event.message.topic_name    
                message = str(event.message.payload, "utf-8")
                
                self.executor.submit(self.message_callback, topic, message)

            except Exception as err: # pragma: no cover 
                log.error('EXCEPTION: Exception Raised from IoT Core on MQTT Subscriber. ERROR MESSAGE: {} - STREAM EVENT: {}'.format(err, event))

        def on_stream_error(self, error: Exception) -> bool:
            log.error('ON_STREAM_ERROR: IoT Core MQTT PubSub Subscriber Stream Error. ERROR MESSAGE: {}'.format(error))
            return True  # Return True to close stream, False to keep stream open.

        def on_stream_closed(self) -> None:
            log.error('ON_STREAM_CLOSED: IoT Core MQTT PubSub Subscriber Closed.')
            pass
