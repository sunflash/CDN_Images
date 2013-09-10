#!/bin/bash
#
# Auto restart node with forever if forever is kill

# forever config

foreverPidFile=/var/run/forever.pid

if [ -f $foreverPidFile ]
then
      echo "Forever is running."
else
      /etc/init.d/nodecdn start
      echo "Forever is down."
      echo "($(date)) -- Forever is down." >> /web/log/node.log
fi