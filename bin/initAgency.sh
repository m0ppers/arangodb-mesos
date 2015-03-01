#!/bin/bash

echo Initializing agency at $1:$2 ...
exec arangosh --javascript.execute `dirname $0`/initAgency.js "$1:$2"
