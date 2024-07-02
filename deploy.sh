#!/bin/bash

export PKG_NAME=tunip

python setup.py sdist upload -r internal

echo "${PKG_NAME} deployment completed!"
