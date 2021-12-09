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

    def __init__(self, ble_mac, receive_message_router, ble_state_change_callback):

        try:

             # __init__ extended Thread class
            Thread.__init__(self)

            log.info('Initialising BleUartPeripheral Device MAC: {}'.format(ble_mac))

            # create the TX queue
            self._tx_queue = Queue()

            self.thread_running = True

            # BLE MAC this BleUartPeripheral object will connect to.
            # Normalise ble-mac to all upper case. 
            self.ble_mac = ble_mac.upper()

            # Set the message callback for passing received Rx Ble Messages.
            self.receive_message_router = receive_message_router

            # Callback to message a change in connectivity state of this BLE Peripheral
            self.ble_state_change_callback = ble_state_change_callback

            self.ble_proxy_topic = '$aws/greengrass/ble/do-proxy/{}'.format(self.ble_mac)

            self._conn_state = { "connection-state": "waiting-init", "addr-type": "N/A" }
         
            log.info('Initialising BleUartPeripheral MAC: {} Complete'.format(self.ble_mac))

        except Exception as err:
            log.error('Exception raised initialising BleUartPeripheral MAC: {} - ERROR MESSAGE: {}'.format(ble_mac, err))
            raise


    def _ble_init_connect(self):
        '''
        Initilise the BLE connection to the Peripheral device
        Note: Will disconect and kill any existing connections / Peripheral device objects.
        '''

        try:
            # Create the BLE Peripheral device and configure 
            log.info('Connecting to BLE Device: {}'.format(self.ble_mac))
            
            # Clear any previously initilised ble_peripheral (and updates connection state).
            self._disconect_ble()

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
            self._ble_peripheral.withDelegate(BleUartDelegate(self.ble_mac, self.ble_proxy_topic, self.receive_message_router))
        
        except Exception as err:
            #  Log the failed BLE connection to this peripheral
            log.info('EXCEPTION Connection state error for BLE device: {} - ERROR-MESSAGE: {}'.format(self.ble_mac, err))

        finally:
            # Update the connection state variable and return
            self._set_ble_connection_state()
            log.info('Connection state for BLE device: {} - {}'.format(self.ble_mac, self._conn_state))

    def _set_ble_connection_state(self):
        '''
        Provides the connection state of this BLE device. Is private as BluePy is not thread safe
        and any request to this method while the thread has an active Rx/Tx proces causes an exception / error.

        If a state change is detected will update via the self.ble_state_change_callback()

        Don't call this function from outside of this thread, it is called in the thread loop 
        in a thread safe way and populates the self._conn_state variable. Access that via 
        get_connection_state() instead or is_connected().
        '''

        try:
            previous_state = self._conn_state.copy()

            if hasattr(self,'_ble_peripheral') and self._ble_peripheral:
                state = self._ble_peripheral.getState()
                self._conn_state =  {'connection-state' : state, 'addr-type' : self._ble_peripheral.addrType}
            else:
                # Used 'disc' = disconnected to match getState() return from BluePy when its initilised
                # https://github.com/IanHarvey/bluepy/blob/master/docs/peripheral.rst
                self._conn_state =  {'connection-state' : 'disc', 'addr-type' : 'N/A'}

        except Exception as err:
            self._conn_state = {'connection-state' : 'error', 'state-request-error' : str(err)}
        
        finally:
            # If connectivt=y state has changed, upfdate status callback.
            if (previous_state != self._conn_state):
                self.ble_state_change_callback(self.ble_mac, self._conn_state, previous_state)

    def _disconect_ble(self):
        '''
        Gracefully disconnect the ble_peripheral. Call close_thread to shut down the entire thread opposed 
        to just the BLE connection when acting from a public call.
        '''
        if hasattr(self,'_ble_peripheral') and self._ble_peripheral:
            log.info('Disconnecting BLE connection on peripheral Mac: {}'.format(self.ble_mac))
            self._ble_peripheral.disconnect()
        
        self._ble_peripheral = None
        self._set_ble_connection_state()

    def is_connected(self):
        return self._conn_state['connection-state'] == 'conn'

    def get_connection_state(self):
        '''
        Returns last update of this BLE Peripheral device conection state in a thread safe way 
        that doesn't interact with the BLE Peripheral connection itself.
        '''
        return self._conn_state
    
    def close_thread(self):
        self.thread_running = False

    def run(self):
        # Note: Bluepy is not thread safe so can't run Rx loop in a seperate thread. Need to Rx and Tx sequentially.
        # Also can't safely poll connection state during read / write operations
        # Have set Rx timeout low (0.1 secs) to avoid long delays.

        # Initilie the BLE Peripheral connection. 
        log.info('Rx/Tx thread started for BLE Device: {}, initilising connection.....'.format(self.ble_mac))
        self._ble_init_connect()

        while self.thread_running:

            try:

                #############################################
                ## Transmit BLE messages
                while not self._tx_queue.empty() and self.thread_running:

                    # Parse and send messages in queue
                    tx_object = self._tx_queue.get_nowait()
                    with_ble_response = tx_object['with_ble_response']
                    message_object = tx_object['message_object']

                    log.info('Publishing To BLE MAC: {} - tx_object: {}'.format(self.ble_mac, tx_object))
                    json_message = json.dumps(message_object) + '\n'
                    payload_bytes = bytes(json_message.encode('utf-8'))
                    self._uart_rx.write(payload_bytes, with_ble_response)

                #############################################
                ## Receive BLE messages - handled by BleUartDelegate (callbacks)
                if self.thread_running:
                    self._ble_peripheral.waitForNotifications(0.1)

            #############################################
            ## Catch Rx/Tx exceptions, mostly expected due to lost BLE connection.
            except Exception as err:
                # If thread still running, attempt to re-initilise the BLE device connection
                if (self.thread_running):
                    log.error('BLE Mac {} error: {} - attempting to reconnect.'.format(self.ble_mac, err))
                    
                    # Give time for the BLE device to disconnect and start re-advertsing beacon 
                    # also prevents fast exceptions loops when device is not reachable.
                    self._disconect_ble()
                    time.sleep(5)

                    # Attempt to reconnect.
                    self._ble_init_connect()

        # Clear the BLE connection if leaving the running thread.
        self._disconect_ble()
        log.info('Exiting the BLE Peripheral Rx/Tx process loop for BLE Mac: {}'.format(self.ble_mac))

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

    def __init__(self, ble_mac, ble_proxy_topic, receive_message_router):

        btle.DefaultDelegate.__init__(self)
        
        self.ble_mac = ble_mac

        self.ble_proxy_topic = ble_proxy_topic

        # Main process BLE message callback / processor
        self.receive_message_router = receive_message_router
        
    def handleNotification(self, cHandle, data):
        
        try:
            log.debug('BLE Notification Received: {}'.format(data))

            # Decode the Bytes to JSON String and pass to receive_message_router
            # To match the PubSub design assign a simulate BLE topic
            json_str = data.decode("utf-8", "ignore")
            self.receive_message_router(self.ble_proxy_topic, json_str)

        except Exception as err:
            log.error('Exception raised in BleUartDelegate handleNotification MAC: {} - ERROR MESSAGE: {}'.format(self.ble_mac, err))
