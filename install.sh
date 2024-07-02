#!/bin/bash

export TUNIP_VERSION=$1
export PKG_SERVER_HOST=0.0.0.0
export PKG_SERVER_PORT=8099

pip -v -q install --extra-index-url http://${PKG_SERVER_HOST}:${PKG_SERVER_PORT}/simple --trusted-host=${PKG_SERVER_HOST} tunip==${TUNIP_VERSION}

echo "tunip-${TUNIP_VERSION} installation completed!"
