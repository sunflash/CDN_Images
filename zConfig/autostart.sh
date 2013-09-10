#!/bin/bash
#
# Auto restart node with forever if forever is kill

# forever config

foreverPidFile=/var/run/forever.pid

if [ -f $foreverPidFile ]
then
      echo "Forever is running."
else
      echo "Forever is down."
      service nodecdn start
fi






