'''
    ble_scanner.py:

    This class provides a Bluetooth BLE scanner to identify any devices in range 
    that are activly advertising thier BLE interface.

    While not compulsary, we recommend and look for ManufactureSpecificIDs 
    in the BLE advertisment to provide a descriptive value of the device.
'''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import logging
from bluepy.btle import Scanner, DefaultDelegate

# Config the logger.
log = logging.getLogger(__name__)

class BleScanner():

    def __init__(self):

        try:
            log.info('Initialising BLE Scanner Class')

            # create a scanner object that sends BLE broadcast packets to the ScanDelegate
            self.scanner = Scanner().withDelegate(ScanDelegate())
            
            log.info('Initialising BLE Scanner Class Complete')

        except Exception as err:
            log.error('Exception raised Initialising BLE Scanner Class - ERROR MESSAGE: {}'.format(err))
            raise

    def scan_ble_devices(self, scan_secs):

        # create a list of unique devices that the scanner discovered during thescan
        devices = self.scanner.scan(scan_secs)

        # Get relevent info for each device found in the scan
        devices_info = {}
        for dev in devices:

            # Creat object for devuce with initil info
            devices_info[dev.addr] = {
                "address-type" : dev.addrType,
                "rssi-db" : dev.rssi
            }
            
    
            # For each of the device's advertising data items, get a description of the data type and value of the data itself
            # getScanData returns a list of tupples: adtype, desc, value
            # where AD Type means “advertising data type,” as defined by Bluetooth convention:
            # https://www.bluetooth.com/specifications/assigned-numbers/generic-access-profile
            # desc is a human-readable description of the data type and value is the data itself
            devices_info[dev.addr]['ad-data-types'] = {}
            for (adtype, desc, value) in dev.getScanData():
                
                if adtype == 255 and value:
                    value = self.int_string_to_chr(value)

                devices_info[dev.addr]['ad-data-types'][adtype] = {
                    "adtype-value" : value,
                    "descption" : desc
                }
        
        return devices_info

            
    ## Helpers 
    def int_string_to_chr(self, value):
        ascii_value = ''
        for i, j in zip(value[::2], value[1::2]):
            ascii_value += chr(int(i+j, 16))
        
        return ascii_value




# create a delegate class to receive the BLE broadcast packets
class ScanDelegate(DefaultDelegate):
    def __init__(self):
        DefaultDelegate.__init__(self)

    # when this python script discovers a BLE broadcast packet, print a message with the device's MAC address
    def handleDiscovery(self, dev, isNewDev, isNewData):
        if isNewDev:
            print('Discovered device: {}'.format(dev.addr))
        elif isNewData:
            print('Received new data from {}'.format(dev.addr))








