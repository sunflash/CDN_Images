#!/bin/bash
#
# Start node with forever

# redis config

redisPort=6379
redisPidFile=/var/run/redis_${redisPort}.pid
redisConfigFile="/etc/redis/${redisPort}.conf"

redis=/usr/local/bin/redis-server

# forever config

foreverPidFile=/var/run/forever.pid
foreverLogDir=/web/log
logFile=$foreverLogDir/forever.log
outFile=$foreverLogDir/out.log
errFile=$foreverLogDir/err.log

sourceDir=/web/imagecdn
indexFile=index.js
scriptId=$sourceDir/$indexFile

forever=/usr/local/bin/forever

export PATH=$PATH:/usr/local/bin
export NODE_PATH=$NODE_PATH:/usr/local/lib/node_modules

start() {

    if [ -f $redisPidFile ]
    then
            echo "Redis is running."
    else
            echo echo "!!! Redis is not running. Starting Redis server.."
            $redis $redisConfigFile
    fi

    echo "Starting $scriptId"
    cd $sourceDir
    $forever start --minUptime 30000 --spinSleepTime 2000 --pidFile $foreverPidFile -a -l $logFile -o $outFile -e $errFile -w --sourceDir $sourceDir/ $indexFile
}

restart() {
    echo "Restarting $scriptId"
    $forever restart $scriptId
}

stop() {
    echo "Shutting down $scriptId"
    $forever stop $scriptId
}

status() {
    echo "Status $scriptId"
    $forever list
}

reload() {

    echo "Reloading $scriptId"

    $forever stopall
    rm $foreverLogDir/*

    cd $sourceDir
    $forever start --minUptime 5000 --spinSleepTime 2000 --pidFile $foreverPidFile -a -l $logFile -o $outFile -e $errFile -w --sourceDir $sourceDir/ $indexFile
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    restart)
        restart
        ;;
    reload)
        reload
        ;;
    *)
        echo "Usage: {start|stop|status|restart|reload}"
        ;;
esac
