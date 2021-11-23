'''
    ble_uart_peripheral.py:

    The Bluetooth (BLE) Peripheral based on Nordic UART Device servuce characteristics.
'''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

# import the necessary parts of the bluepy library
import json
import logging
import time
import bluepy.btle as btle
from queue import Queue
from threading import Thread

# Config the logger.
log = logging.getLogger(__name__)

# GATT service IDs and Rx/Tx Characteristics for Nordic UART.
_UART_SERVICE_UUID = "6E400001-B5A3-F393-E0A9-E50E24DCCA9E"
_UART_RX_CHAR_UUID = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"
_UART_TX_CHAR_UUID = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"

class BleUartPeripheral(Thread):

    def __init__(self, ble_mac, ble_message_callback):

        try:
            log.info('Initialising BleUartPeripheral Device MAC: {}'.format(ble_mac))

            # __init__ extended Thread class
            Thread.__init__(self)

            # create the TX queue
            self._tx_queue = Queue()

            self.thread_running = True

            # BLE MAC this BleUartPeripheral object will connect to.
            # Normalise ble-mac to all upper case. 
            self.ble_mac = ble_mac.upper()

            self.ble_message_callback = ble_message_callback

            self.ble_proxy_topic = '$aws/greengrass/ble/do-proxy/{}'.format(self.ble_mac)
         
            log.info('Initialising BleUartPeripheral MAC: {} Complete'.format(self.ble_mac))

        except Exception as err:
            log.error('Exception raised initialising BleUartPeripheral MAC: {} - ERROR MESSAGE: {}'.format(ble_mac, err))
            raise

    def ble_init_connect(self):

        try:
            # Create the BLE Peripheral device and configure 
            log.info('Connecting to BLE Device: {}'.format(self.ble_mac))
            
            # re/initialise the ble_peripheral and connect to the device
            self._ble_peripheral = btle.Peripheral(self.ble_mac)
            self._ble_peripheral.setMTU(5000)

            log.info('Getting _uart_service for BLE device: {}'.format(self.ble_mac))
            self._uart_service = self._ble_peripheral.getServiceByUUID(_UART_SERVICE_UUID)
            log.info('_uart_service: {}'.format(self._uart_service))

            # get the _uart_rx characteristic, should only ever get one in list against UUID
            log.info('Getting _uart_service Rx Characteristic for BLE device: {}'.format(self.ble_mac))
            self._uart_rx = self._uart_service.getCharacteristics(forUUID=_UART_RX_CHAR_UUID)[0]
            log.info('_uart_service Rx Characteristic: {}'.format(self._uart_rx))

            # get the _uart_tx characteristic, should only ever get one in list against UUID
            log.info('Getting _uart_service Tx Characteristic for BLE device: {}'.format(self.ble_mac))
            self._uart_tx  = self._uart_service.getCharacteristics(forUUID=_UART_TX_CHAR_UUID)[0]
            log.info('_uart_service Tx Characteristic: {}'.format(self._uart_tx))

            # Once the BLE Peripheral is initialised then add the RX Delegate. 
            self._ble_peripheral.withDelegate(BleUartDelegate(self.ble_mac, self.ble_proxy_topic, self.ble_message_callback))

            # Set the initial connection state 
            log.info('Getting the connection state for BLE device: {}'.format(self.ble_mac))
            self._set_ble_connection_state()
            log.info('Connection state for BLE device: {} - {}'.format(self.ble_mac, self.conn_state))

            log.info('Connecting to BLE Device: {} Successful'.format(self.ble_mac))

        except Exception as err:
            log.error('Exception initialising / connecting BleUartPeripheral MAC: {} - ERROR MESSAGE: {}'.format(self.ble_mac, err))
            raise

    def close_thread(self):
        self.thread_running = False
        self._ble_peripheral.disconnect()

    def get_connection_state(self):
        '''
        Returns last update of this devuces conectio state in a thread safe way 
        that doesn't interact with the BLE device itself. 
        '''
        return self.conn_state
    
    def _set_ble_connection_state(self):
        '''
        Provides the connection state of this BLE device. Is private as BluePy is not thread safe (at all!)
        and any request to this method while the thread has an active Rx/Tx loop causes the devices to lock up.

        DO NOT call this function from outside of this thread, it is called in the thread loop 
        in a thread safe way and populates the self.conn_state variable. Access that via 
        get_connection_state() instead.
        '''
        
        try:
            state = self._ble_peripheral.getState()
            self.conn_state =  {'connection-state' : state, 'addr-type' : self._ble_peripheral.addrType}
        
        except Exception as err:
            self.conn_state = {'connection-state' : 'error', 'state-request-error' : str(err)}

    def run(self):
        # Note: Bluepy is not thread safe so can't run Rx loop in a seperate thread. Need to Rx and Tx sequentially
        # Have set Rx timeout low (0.1 secs) to avoid long delays. Not ideal, needs uppler layers to validate responses.
        while self.thread_running:
            try:

                ## Transmit any messages in queue
                while not self._tx_queue.empty():

                    # Parse and send messages in queue
                    tx_object = self._tx_queue.get_nowait()
                    with_ble_response = tx_object['with_ble_response']
                    message_object = tx_object['message_object']

                    log.info('Publishing To BLE MAC: {} - tx_object: {}'.format(self.ble_mac, tx_object))
                    json_message = json.dumps(message_object) + '\n'
                    payload_bytes = bytes(json_message.encode('utf-8'))
                    self._uart_rx.write(payload_bytes, with_ble_response)

                ## Receive notification
                self._ble_peripheral.waitForNotifications(0.1)

            except Exception as err:
                # On exception try and re-initilise the BLE device connection. 
                log.error('EXCEPTION: BLE Publish - attempt to reconnect ERROR: {}'.format(err))
                self._set_ble_connection_state()
                self._ble_peripheral.disconnect()
                self.ble_init_connect()

    def publish_to_ble(self, message_object, with_ble_response=True):
        '''
            Creates a message TX_object from the parameterised values and submits to the 
            tx_message queue. message_object must be a Pythin Object that will be 
            seralised to JSON. 

            Params:
            message_object: Object to seralise to JSON and publish.

            with_ble_response (Boolean):
            Await confirmation that the write was successful from the device. 
        '''

        tx_object = {
            "with_ble_response": with_ble_response,
            "message_object" : message_object
        }
        self._tx_queue.put_nowait(tx_object)

class BleUartDelegate(btle.DefaultDelegate):

    def __init__(self, ble_mac, ble_proxy_topic, ble_message_callback):

        btle.DefaultDelegate.__init__(self)
        
        self.ble_mac = ble_mac

        self.ble_proxy_topic = ble_proxy_topic

        # Main process BLE message callback / processor
        self.ble_message_callback = ble_message_callback
        
    def handleNotification(self, cHandle, data):
        
        try:
            log.debug('BLE NOTIFICATION RECEIVED: {}'.format(data))

            # Decode the Bytes to JSON String and pass to ble_message_callback
            # To match the PubSub design assign a simulate BLE topic
            json_str = data.decode("utf-8", "ignore")
            self.ble_message_callback(self.ble_proxy_topic, json_str)

        except Exception as err:
            log.error('Exception raised in BleUartDelegate handleNotification MAC: {} - ERROR MESSAGE: {}'.format(self.ble_mac, err))
