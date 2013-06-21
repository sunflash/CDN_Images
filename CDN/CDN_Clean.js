/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 21/06/13
 * Time: 10.57
 * To change this template use File | Settings | File Templates.
 */

var async = require("async");
var redis = require("redis"),
    client = redis.createClient();

var cdnAPI = require('./CDN_API');

var cdnCleanKey = 'cdn.clean';
var cleanFlow = async.compose(cleanExpiredDataInRedisDB,deleteExpiredContainer,disableExpiredContainerInCDN,getContainerWithExpiredPubID);

exports.cleanExpiredDataInCloudFileAndCDN = function cleanExpiredDataInCloudFileAndCDN (callback) {

    client.SMEMBERS(cdnCleanKey, function(err, cleanPubID) {

        if(err) callback(null);
        else if (cleanPubID && cleanPubID.length > 0) {

            cleanFlow(cleanPubID, function (err, result) {

                if (err) {

                    console.log(err);
                    callback(null);
                }
                else if (!result)  {

                    console.log('No container with expired pubID, no clean up needed');
                    callback(null);
                }
                else if (result.length > 0) {

                    console.log('Remove '+result.length+' containers with expired data in CDN');
                    callback(result);
                }
            });
        }
        else callback(null);
    });

    console.log('***** Clean old data in Cloud File and CDN ***** '+ new Date());
}

var cdnRedisKeyPrefix = 'CDN.';

function getContainerWithExpiredPubID (cleanPubID, callback) {

    var expiredContainerKeys = [];

    async.each(cleanPubID,function(expirePubID, next) {

            client.KEYS((cdnRedisKeyPrefix+expirePubID+'.*'), function(err, expiredCDNContainerKey) {

                if(err) next(err);
                else if (expiredCDNContainerKey && expiredCDNContainerKey.length > 0) {

                    expiredContainerKeys = expiredContainerKeys.concat(expiredCDNContainerKey);
                    next();
                }
                else next();
            });
        }
        ,function(err) {

            if (err) callback(err);
            else if (expiredContainerKeys.length > 0) {

                callback(null,cleanPubID,expiredContainerKeys);
                expiredContainerKeys = null;
            }
            else callback(null,cleanPubID,null);
    });
}

function disableExpiredContainerInCDN (cleanPubID,expiredContainerInfo, callback) {

    if (expiredContainerInfo && expiredContainerInfo.length > 0 ) {

        async.each(expiredContainerInfo, function(expiredContainerKey, next){

                var info = expiredContainerKey.split('.');
                var containerName = info[1]+'_'+info[2];

                cdnAPI.cdnDisableContainer(containerName, function(containerDetails) {

                    next();
                    info = null;
                    containerName = null;
                    containerDetails = null;
                });
            }
            ,function(err) {

                callback(null,cleanPubID,expiredContainerInfo);
                err = null;
            }
        );
    }
    else {

        callback(null,cleanPubID,null);
    }
}

function deleteExpiredContainer (cleanPubID,expiredContainerInfo, callback) {

    if (expiredContainerInfo && expiredContainerInfo.length > 0 ) {

        async.each(expiredContainerInfo, function(expiredContainerKey, next) {

                var info = expiredContainerKey.split('.');
                var containerName = info[1]+'_'+info[2];

                cdnAPI.deleteContainers(containerName, function(containerDetails) {

                    if (containerDetails == 1) {
                        next();
                    }
                    else next('Delete container '+containerName+' failed');

                    info = null;
                    containerName = null;
                });
            }
            ,function(err) {

                if(err) callback(err);
                else    callback(null,cleanPubID,expiredContainerInfo);
            }
        );
    }
    else {

        callback(null,cleanPubID,null);

    }
}

function cleanExpiredDataInRedisDB (cleanPubID,expiredContainerInfo, callback) {

    if (expiredContainerInfo && expiredContainerInfo.length > 0 ) {

        async.each(expiredContainerInfo, function(expiredContainerKey, next) {

                client.DEL(expiredContainerKey, function(err, obj) {

                    if(err) next(err);
                    else    next();

                    obj = null;
                });
            }
            ,function(err) {

                if (cleanPubID && cleanPubID.length > 0) {

                    client.DEL(cdnCleanKey, function(err,obj) {

                        if(err) callback(err);
                        obj = null;
                    });
                }

                if(err) callback(err);
                else    callback(null,expiredContainerInfo);
            }
        );
    }
    else {

        if (cleanPubID && cleanPubID.length > 0) {

            client.DEL(cdnCleanKey, function(err,obj) {

                if(err) callback(err);
                obj = null;
            });
        }

        callback(null,null);
    }
}
