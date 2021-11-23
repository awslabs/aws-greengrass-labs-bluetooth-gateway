'''
main.py:

Main / bootstrap MicroPython for ESP32 Bluetooth Low Energy (BLE)
Device using the Nordic UART BLE GATT Service to connect with AWS Greengrass 
BLE connector component.

'''

__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import esp32
import uos
import gc
import utime as time
import json
import machine
from machine import Pin

from ble.ble_uart_peripheral import BleUartPeripheral

class ESP32_BLE_Processor():

    def __init__(self, device_name):
        '''
            Initialises this ESP32 BLE Device
        '''
        
        try:
            #######################################################
            # Log the start of the process
            print('Initialising ESP32 uProcessor BLE Device')

            super().__init__()

            # Descriptive name for this assembly
            self.device_name = device_name

            # Set the LED Pin
            self.pin_led  = Pin(2, Pin.OUT)

            # Init the BleUartPeripheral
            self.ble_peripheral = BleUartPeripheral()
            self.ble_peripheral.irq(handler=self.ble_message_callback)

            # TODO: Renmove, only for testing.
            self.disable_stepper_pins()


        except ValueError as val_error: # includes JSON parsing errors
            err_msg =  'VAL_ERROR: Initialising ESP32 BLE Device. JSON Parsing Error / Unexpected message format. ERROR MESSAGE: {}'.format(val_error)
            self.publish_exception(500, err_msg)

        except KeyError as key_error: # includes requests for fields that don't exist in the received config
            err_msg = 'KEY_ERROR: Initialising ESP32 BLE Device. Missing required fields. ERROR MESSAGE: {}'.format(key_error)
            self.publish_exception(500, err_msg)

        except Exception as err:
            err_msg = 'EXCEPTION: Exception initialising ESP32 BLE Device. ERROR MESSAGE: {}'.format(err)
            self.publish_exception(500, err_msg)
        
        finally:
            gc.collect()
    
    ##################################################
    ### BLE / PubSub Topic Message Callback
    ##################################################

    def ble_message_callback(self):
        '''
            Main Callback for all received BLE Messages. Triggered by BLE IRQ. 
            Reads in the message, provides initial message validation and 
            forwards to the message routers.
            
            Expects message payload provided is JSON that can be Serialized and 
            Deserialized to a programmatic language specific object in the 
            prescribed message formats.
        '''

        try:

            # Read in BLE message from ble_peripheral
            payload = self.ble_peripheral.read().decode().strip()

            # Debug unprocessed message
            print('BLE RECEIVED MESSAGE: {}\n'.format(payload))

            # Json loads the message
             # Note: This example expects all messages to be valid JSON format
            message = json.loads(payload)
            self.ble_request_message_router(message)

        except ValueError as val_error: # includes JSON parsing errors
            err_msg = {'error-message' : 'VAL_ERROR: JSON Parsing Error / Unexpected PubSub message format received. ERROR MESSAGE {}'.format(val_error)}
            self.publish_exception(500, err_msg)

        except KeyError as key_error: # includes requests for fields that don't exist in the received object
            err_msg = {'error-message' : 'KEY_ERROR: Received PubSub message missing required fields. ERROR MESSAGE {}'.format(key_error)}
            self.publish_exception(500, err_msg)

        except Exception as err:
            err_msg = {'error-message' : 'EXCEPTION from BLE message callback. ERROR MESSAGE {}'.format(err)}
            self.publish_exception(500, err_msg)

        finally:
            gc.collect()

    def disable_stepper_pins(self):
        '''
        This just to disable connected stepper motor during testing
        '''

        # Set the active/enabled parameter.
        self.active_enable = 1

        # Initilise stepper-motor controller pins
        self.pin_enb = Pin(23, Pin.OUT)
        self.pin_dir = Pin(22, Pin.OUT)
        self.pin_step = Pin(21, Pin.OUT)
        self.pin_led  = Pin(2, Pin.OUT)

        # Init control pins to 0
        self.pin_dir.value(0)
        self.pin_step.value(0)
        self.pin_led.value(0)

        # Stepper controller has enable as well as power that needs 3.3v feed
        # is opto-coupled so can run off a GPIO. Set pin and turn to on
        self.pin_step_ctl_pwr = Pin(19, Pin.OUT)
        self.pin_step_ctl_pwr.value(1)

        # Disable Stepper until a command is received
        self.pin_enb.value(not self.active_enable)

    ##########################################################
    # BLE Message Routers
    def ble_request_message_router(self, message):

        try:
            command = message['command']

            # Action based on command.
            if command == 'describe-ble-device':
                self.describe_ble_device()

            elif command == 'toggle_led':
                self.toggle_led()
                
            elif command == 'hw-reset-micro':
                self.hw_reset_micro()

            elif command == 'get-processor-board-temp':
                self.publish_board_temp()

            else:
                err_msg = 'Unknown BLE Command: {} REQUEST received'.format(command)
                self.publish_exception(404, err_msg)

        except Exception as err:
            err_msg = {'error_message' : 'EXCEPTION from BLE ble_request_message_router. ERROR MESSAGE {}'.format(str(err))}
            self.publish_exception(500, err_msg)

    ##########################################################
    # BLE Message Processors

    def describe_ble_device(self):
        
        try:
            message = {
                "status" : 200,
                "data" : {
                    'uname': '\"{}\"'.format(uos.uname())
                }
            }
            self.publish_message(message)

        except Exception as err:
            err_msg = {'error-message' : 'EXCEPTION in describe_ble_device. ERROR MESSAGE {}'.format(err)}
            self.publish_exception(500, err_msg)
    
    def toggle_led(self):
        
        try:

            self.pin_led.value(not self.pin_led.value())
            message = {
                "status" : 200,
                "data" : {
                    "message" :  "ESP32 BLE LED Set to: {}".format(self.pin_led.value())
                }
            }
            self.publish_message(message)

        except Exception as err:
            err_msg = {'error-message' : 'EXCEPTION in toggle_led. ERROR MESSAGE {}'.format(err)}
            self.publish_exception(500, err_msg)

    def hw_reset_micro(self):
        
        try:
            message = {
                "status" : 200,
                "data" : {
                    "message" :  "ESP32 BLE Device reboot request accepted"
                }
            }

            self.publish_message(message)
            # Once response sent, reset the processor. 
            machine.reset()

        except Exception as err:
            err_msg = {'error-message' : 'EXCEPTION in hw_reset_micro. ERROR MESSAGE {}'.format(err)}
            self.publish_exception(500, err_msg)

    def publish_board_temp(self):

        try:
            temp_f = esp32.raw_temperature()
            temp_c = round((temp_f-32.0)/1.8, 2)

            message = {
                "status" : 200,
                "data" : {
                    "board_temp_celsius" :  temp_c,
                    "board_temp_fahrenheit" :  temp_f
                }
            }
            self.publish_message(message)

        except Exception as err:
            err_msg = {'error-message' : 'EXCEPTION in publish_board_temp. ERROR MESSAGE {}'.format(err)}
            self.publish_exception(500, err_msg)

    ##################################################
    ### BLE Message Publisher
    ##################################################

    def publish_message(self, message):
        '''
            Callback for BLE Message Publish
        '''

        try:

            self.ble_peripheral.write(message) 
        
        except Exception as err:
            # Return an error message
            err_msg = 'EXCEPTION: Error Publishing ERROR: {} - MESSAGE: {}'.format(err)
            self.publish_exception(500, err_msg)

    def publish_exception(self, error_status, error_message):
        '''
            Publish an exception / error back to BLE controller. 
        '''

        # Print the error
        print(error_message)

        # Publish the error to BLE Controller for logging / handling
        err_message = {
            "status" : error_status,
            "data" : {
                "error-message" : error_message
            }
        }
        self.ble_peripheral.write(err_message) 

    ##################################################
    # Main service / process application logic
    ##################################################

    def service_loop(self):

        # Apply application code here for periodic / looped functions or
        # leave as a slow loop to hold the process up to process event driven 
        # BLE / GPIO / IRQ notifications.
        try:
            while True:

                # Just do slow loop to hold up process
                time.sleep(10)

        except Exception as err:
            self.publish_exception(500, 'EXCEPTION: Service loop - ERROR MESSAGE: {}'.format(err))

if __name__ == '__main__':
    
    # Init the uController / ESP32 Bluetooth (BLE) module.
    device_name = 'BLE_DeviceXX'
    esp32_ble = ESP32_BLE_Processor(device_name)

    # The main process loop, add application logic in this function.
    esp32_ble.service_loop()

