'''
    ble_uart_controller.py:

    This class manages the mapping and communications with Bluetooth BLE devices.

    This class can scan and connect to BLE devices on request. Each connected BLE 
    device initialises an instance of ble_uart_peripheral which contains a method to 
    publish to the BLE device and a Rx loop that forwards any received messages 
    to the ble_message_callback provided.

    All messages to DOF assemblies are expected to be JSON formatted and are described 
    by the uProcessor code in each assembly. 
'''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

# import the necessary parts of the bluepy library
import json
import time
import logging

import bluepy
import bluepy.btle as btle
from ble.ble_uart_peripheral import BleUartPeripheral

# Config the logger.
log = logging.getLogger(__name__)

class BleUartController():

    def __init__(self, ble_message_callback):

        try:
            log.info('Initialising BleController')


            # Initialise the connected BLE Device instance object.
            # A dict with the BLE MAC as the key / index providing access to 
            # the ble_uart_peripheral instance holding the connection to this device. 
            # i.e: ble_devices = {
            #   "ac:67:b2:3c:92:06" : ble_uart_peripheral instance01,
            #   "ac:67:b2:3c:92:07" : ble_uart_peripheral instance02,
            #   "ac:67:b2:3c:92:08" : ble_uart_peripheral instance03,
            #   }
            self.ble_devices = {}

            # Callback to pass messages received on a BLE interface.
            self.ble_message_callback = ble_message_callback

        except Exception as err:
            log.error('EXCEPTION: Exception raised initialising BleController. ERROR MESSAGE: {}'.format(err))
            raise

    def connect_ble_device(self, ble_mac):
        '''
            Creates a connection to the given BLE MAC address and initialises the BleUartPeripheral
            instance that holds the connection and BLE notification receive loop. 

            Note: Is recommended to scan and verify a BLE device exists and is reachable before 
            calling this method as takes ~40 secs for the connect to fail and return a response.
        '''

        # Create a BleUartPeripheral object for the requested BLE device
        try:
            # Normalise ble-mac to all upper case.
            ble_mac = ble_mac.upper()

            log.info('Connecting to BLE Device: {}'.format(ble_mac))

            # if reconnecting to a device already in the active device list then 
            # gracefully disconnect and remove the device first.
            if ble_mac in self.ble_devices:
                self.disconnect_ble_device(ble_mac)

            # Create the BleUartPeripheral class for this device.
            ble_device = BleUartPeripheral(ble_mac, self.ble_message_callback)

            # Initilise and connect to the BLE device
            ble_device.ble_init_connect()
            
            # Starts the BLE Rx/Tx thread loop to accept and send messages.  
            ble_device.start()

            # If device starts / connects with no error then add to local list
            self.ble_devices[ble_mac] = ble_device

            log.info('Connecting to BLE Device: {} Successful'.format(ble_mac))

            return {'status': 200, 'data' : {'ble-mac' : ble_mac, 'connect-status' : 'success'}}

        except Exception as err:
            log.error('EXCEPTION: Error connecting to BLE Device: {} - ERROR-MESSAGE: {}'.format(ble_mac, err))
            return {'status': 500, 'data' : {'ble-mac' : ble_mac, 'connect-status' : 'failed', 'error' : str(err)}}

    def disconnect_ble_device(self, ble_mac):
        '''
            Disconnects a BLE Device gracefully and cleans up any resources.
        '''

        # Disconnect and clean up a BleUartPeripheral object for the requested BLE device
        try:
            # Normalise ble_mac to all upper case.
            ble_mac = ble_mac.upper()

            log.info('Disconnecting the BLE Device: {}'.format(ble_mac))

            # If the device doesn't exist, just return disconnect success.
            if not ble_mac in self.ble_devices:
                return {'status': 200,  'data' : {'ble_mac' : ble_mac, 'disconnect-status' : 'success', 'message' : 'Device connection didn\'t exist'}}

            # Get the BleUartPeripheral class for this device.
            ble_device = self.ble_devices[ble_mac]

            # Disconnect and clean up the BLE device thread
            ble_device.close_thread()

            # Pop the device from the active device list
            self.ble_devices.pop(ble_mac, None)

            return {'status': 200, 'data' : {'ble-mac' : ble_mac, 'disconnect-status' : 'success'}}
            
        except Exception as err:
            log.error('EXCEPTION: Error disconnecting the BLE Device: {} - ERROR-MESSAGE: {}'.format(ble_mac, err))
            return {'status': 500, 'data' : {'ble-mac' : ble_mac, 'disconnect-status' : 'failed', 'error' : str(err)}}

    def get_active_ble_devices(self):
        '''
            Returns the connected BLE devices or a error object
        '''
        try:

            response = {}
            for ble_mac, ble_device in self.ble_devices.items():
                response[ble_mac] = ble_device.get_connection_state()

            return {'status': 200, 'data' : response}

        except Exception as err:
            log.error('EXCEPTION: Error reading BLE Device State - ERROR-MESSAGE: {}'.format(err))
            return {'status': 500, 'data' : {'error-message' : str(err)}}
        
    def publish_to_ble(self, ble_mac, message_object, with_ble_response=True):
        '''
        Publishes a message to the BLE Device

        Params:
            ble-mac (string):
            BLE MAC address of the device to publish the message too.

            message_object (Python Object):
            Python object to be JSON serialised and transmitted to the BLE device

            with_ble_response (Boolean):
            Wait BLE Protocol level confirmation that the write was successful from the device. 
        '''

        try:
            # Normalise ble_mac to all upper case.
            ble_mac = ble_mac.upper()

            # Check is a known / connected BLE Device
            if ble_mac in self.ble_devices:
                # Get the BLE Peripheral Device instance
                ble_device = self.ble_devices[ble_mac]

                # Publish to the BLE Peripheral Device.
                ble_device.publish_to_ble(message_object, with_ble_response)

            else:
                raise Exception('Can\'t publish. BLE Device MAC unknown or not connected.')

        except bluepy.btle.BTLEDisconnectError as btleErr:
            raise bluepy.btle.BTLEDisconnectError('BTLEDisconnectError: Publishing to BLE Device Disconnected: {} - DEVICE MAC: {} - MESSAGE: {}'.format(btleErr, ble_mac, message_object))
        
        except Exception as err:
            raise Exception('ERROR publishing to BLE Device: {} - DEVICE MAC: {} - MESSAGE: {}'.format(err, ble_mac, message_object))
