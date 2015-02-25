#!/bin/bash

echo Initializing agency at $1 ...
arangosh --javascript.execute `dirname $0`/discover.js "$1"
