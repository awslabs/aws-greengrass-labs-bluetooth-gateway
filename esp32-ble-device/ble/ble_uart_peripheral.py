'''
ble_uart_peripheral.py:

Creates a Bluetooth 4.2 (BLE) peripheral implementing the Nordic UART Service (NUS).
'''

__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__version__ = "0.0.1"
__email__ = "dean.colcott@gmail.com"
__status__ = "Development"

import json
import bluetooth
import ubinascii
from micropython import const
from ble.ble_advertising import advertising_payload

# Init the IRQ statics
_IRQ_CENTRAL_CONNECT = const(1)
_IRQ_CENTRAL_DISCONNECT = const(2)
_IRQ_GATTS_WRITE = const(3)

# BLE GATT Characteristics
_FLAG_READ = const(0x0002)
_FLAG_WRITE_NO_RESPONSE = const(0x0004)
_FLAG_WRITE = const(0x0008)
_FLAG_NOTIFY = const(0x0010)

# Init the BLE Service and Characteristics for Nordic UART
_UART_UUID = bluetooth.UUID("6E400001-B5A3-F393-E0A9-E50E24DCCA9E")
_UART_TX = (
    bluetooth.UUID("6E400003-B5A3-F393-E0A9-E50E24DCCA9E"),
    _FLAG_NOTIFY,
)
_UART_RX = (
    bluetooth.UUID("6E400002-B5A3-F393-E0A9-E50E24DCCA9E"),
    _FLAG_WRITE,
)
_UART_SERVICE = (
    _UART_UUID,
    (_UART_TX, _UART_RX),
)

# org.bluetooth.characteristic.gap.appearance.xml
_ADV_APPEARANCE_GENERIC_COMPUTER = const(128)

class BleUartPeripheral:

    def __init__(self, ble_name="BLE_Device", ble_specific_id='BLE_Device', rxbuf=1500, mtu=512):
       
        try:
            self._ble = bluetooth.BLE()
            self._ble.active(True)
            self._ble.irq(self._irq)
            ((self._tx_handle, self._rx_handle),) = self._ble.gatts_register_services((_UART_SERVICE,))

            # Get the BLE MAC address for this device so can insert into publish messages
            # to identify this BLE device to receiving functions.
            # This is used by the main function
            # TODO: Put a check that the MAC is PUBLIC (type 0) so can be sure 
            # it doesn't change.
            ble_mac_bytes = self._ble.config('mac')[1]
            ble_mac_hex = ubinascii.hexlify(ble_mac_bytes, ':')
            self.ble_mac = str(ble_mac_hex, 'utf-8').upper()

            # Increase the size of the rx buffer and enable append mode.
            self._ble.config(mtu=mtu)
            self._ble.gatts_set_buffer(self._rx_handle, rxbuf, True)
            self._connections = set()
            self._rx_buffer = bytearray()
            self._handler = None

            # Optionally add services=[_UART_UUID], but this is likely to make the payload too large.
            self._payload = advertising_payload(name=ble_name, manufacturer_specific=ble_specific_id, appearance=_ADV_APPEARANCE_GENERIC_COMPUTER)
            self._advertise()

        except Exception as err:
           raise Exception('EXCEPTION: Exception initialising ESP32 BleUartPeripheral. ERROR MESSAGE: {}'.format(err))
  
    def irq(self, handler):
        self._handler = handler

    def _irq(self, event, data):
        # Track connections so we can send notifications.
        if event == _IRQ_CENTRAL_CONNECT:
            conn_handle, _, _ = data
            self._connections.add(conn_handle)
            print('IRQ_CENTRAL_CONNECT RECEIVED: {}'.format(self._connections))
        
        elif event == _IRQ_CENTRAL_DISCONNECT:
            conn_handle, _, _ = data
            if conn_handle in self._connections:
                self._connections.remove(conn_handle)
            print('_IRQ_CENTRAL_DISCONNECT RECEIVED: {}'.format(self._connections))

            # Start advertising again to allow a new connection.
            self._advertise()

        elif event == _IRQ_GATTS_WRITE:
            conn_handle, value_handle = data
            if conn_handle in self._connections and value_handle == self._rx_handle:
                self._rx_buffer += self._ble.gatts_read(self._rx_handle)
                if self._handler:
                    self._handler()

    def any(self):
        return len(self._rx_buffer)

    def read(self, sz=None):
        if not sz:
            sz = len(self._rx_buffer)
        result = self._rx_buffer[0:sz]
        self._rx_buffer = self._rx_buffer[sz:]
        return result

    def write(self, message_object):
        print('BLE WRITE DATA: {}\n'.format(message_object))

        # Parse message object to JSON
        json_message = json.dumps(message_object) + "\n"
        json_bytes = bytes(json_message.encode('utf-8'))

        for conn_handle in self._connections:
            self._ble.gatts_notify(conn_handle, self._tx_handle, json_bytes)

    def close(self):
        for conn_handle in self._connections:
            self._ble.gap_disconnect(conn_handle)
        self._connections.clear()

    def _advertise(self, interval_us=100000):
        print("STARTED BLE ADVERTISMENT")
        self._ble.gap_advertise(interval_us, adv_data=self._payload)
