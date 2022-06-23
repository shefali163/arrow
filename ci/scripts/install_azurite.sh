#!/bin/bash

# Error out on first failure
set -e

apt-get -y install nodejs
echo "node version = `node --version`"
npm install -g azurite
AZURITE_DIR=${1}/azurite
mkdir $AZURITE_DIR
which azurite
echo "azurite version = `azurite --version`"

# Start azurite
azurite --silent --location $AZURITE_DIR --debug $AZURITE_DIR/debug.log