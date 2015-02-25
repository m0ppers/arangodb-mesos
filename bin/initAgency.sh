#!/bin/bash

echo Initializing agency at $1:$2 ...
arangosh --javascript.execute `dirname $0`/initAgency.js "$1:$2"
