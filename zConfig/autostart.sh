#!/bin/bash
#
# Auto restart node with forever if forever is kill

# forever config

foreverPidFile=/var/run/forever.pid

if [ -f $foreverPidFile ]
then
      echo "Forever is running." > /web/foreverStat.log
else
      echo "Forever is down." > /web/foreverStat.log
fi






