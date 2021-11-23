#!/bin/bash
#
# This is used to deploy components locally 
# (Opposed to managing from AWS IoT Console) and is typically only used during development.
#

if [ "$EUID" -ne 0 ]; then
  echo "This script must be run as root" 
  exit
fi

/greengrass/v2/bin/greengrass-cli deployment create \
  --recipeDir ../recipes \
  --artifactDir ../src/ \
  --merge "aws.greengrass.labs.iot-bluetooth-gateway=0.0.1" 
