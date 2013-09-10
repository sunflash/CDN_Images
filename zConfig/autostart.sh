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
      echo "($(date)) -- Forever is down." >> /web/log/node.log
      service nodecdn start
fi






