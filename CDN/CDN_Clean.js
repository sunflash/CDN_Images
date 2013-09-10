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

var cdnRedisKeyPrefix = 'CDN.';

function getContainerWithExpiredPubID (cleanPubID, callback) {

    var expiredContainerKeys = [];

    async.each(cleanPubID,function(expirePubID, next) {

            client.KEYS((cdnRedisKeyPrefix+expirePubID+'.*'), function(err, expiredCDNContainerKey) {

                if(err) {next(err);}
                else if (expiredCDNContainerKey && expiredCDNContainerKey.length > 0) {

                    expiredContainerKeys = expiredContainerKeys.concat(expiredCDNContainerKey);
                    next();
                }
                else {next();}
            });
        },function(err) {

            if (err) {callback(err);}
            else if (expiredContainerKeys.length > 0) {

                callback(null,cleanPubID,expiredContainerKeys);
            }
            else {callback(null,cleanPubID,null);}
    });
}

function disableExpiredContainerInCDN (cleanPubID,expiredContainerInfo, callback) {

    if (expiredContainerInfo && expiredContainerInfo.length > 0 ) {

        async.each(expiredContainerInfo, function(expiredContainerKey, next){

                var info = expiredContainerKey.split('.');
                var containerName = info[1]+'_'+info[2];

                cdnAPI.cdnDisableContainer(containerName, function(containerDetails) {

                    next();
                    containerDetails = null;
                });
            },function(err) {

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

                    next();

                    if (containerDetails) {}
                });
            },function(err) {

                if(err) {callback(err);}
                else    {callback(null,cleanPubID,expiredContainerInfo);}
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

                    if(err) {next(err);}
                    else    {next();}

                    obj = null;
                });
            },function(err) {

                if (cleanPubID && cleanPubID.length > 0) {

                    client.DEL(cdnCleanKey, function(err,obj) {

                        if(err) {callback(err);}
                        obj = null;
                    });
                }

                if(err) {callback(err);}
                else    {callback(null,expiredContainerInfo);}
            }
        );
    }
    else {

        if (cleanPubID && cleanPubID.length > 0) {

            client.DEL(cdnCleanKey, function(err,obj) {

                if(err) {callback(err);}
                obj = null;
            });
        }

        callback(null,null);
    }
}

var cleanFlow = async.compose(cleanExpiredDataInRedisDB,deleteExpiredContainer,disableExpiredContainerInCDN,getContainerWithExpiredPubID);

exports.cleanExpiredDataInCloudFileAndCDN = function cleanExpiredDataInCloudFileAndCDN (callback) {

    client.SMEMBERS(cdnCleanKey, function(err, cleanPubID) {

        if(err) {callback(null);}
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
        else {callback(null);}
    });

    console.log('***** Clean old data in Cloud File and CDN ***** '+ new Date());
};

function cleanCatalogDataInRedisDB (cleanPubID,expiredContainerInfo, callback) {

    if (expiredContainerInfo && expiredContainerInfo.length > 0 ) {

        async.each(expiredContainerInfo, function(expiredContainerKey, next) {

                client.DEL(expiredContainerKey, function(err, obj) {

                    if(err) {next(err);}
                    else    {next();}

                    obj = null;
                });
            },function(err) {

                if(err) {callback(err);}
                else    {callback(null,expiredContainerInfo);}
            }
        );
    }
    else {callback(null,null);}

    cleanPubID = null;
}

exports.cleanCloudFileAndCDNCatalogData = function cleanCloudFileAndCDNCatalogData (cleanPubID, callback) {

    var cleanCloudFileAndCDNCatalogDataFlow = async.compose(cleanCatalogDataInRedisDB,deleteExpiredContainer,disableExpiredContainerInCDN,getContainerWithExpiredPubID);

    cleanCloudFileAndCDNCatalogDataFlow(cleanPubID, function (err, result) {

        if (err) {

            console.log(err);
            callback(null);
        }
        else if (!result)  {

            console.log('No container with pubID, no clean up needed');
            callback(null);
        }
        else if (result.length > 0) {

            console.log('Remove '+result.length+' containers with expired data in CDN');
            callback(result);
        }
    });
};

function getAllContainer (callback) {

    cdnAPI.containerList(function (containerList) {

        var containerNames = [];

        if (containerList && containerList.length > 0) {

            async.each(containerList,function(containerInfo, next) {

                if (containerInfo.name.indexOf('_') !== -1) {

                    containerNames.push(containerInfo.name);
                }
                next();

            },function(err) {

                if (err)    {callback(err);}
                else        {callback(null,containerNames);}
            });
        }
        else {callback(null,containerNames);}
    });
}

function disableContainer (containerNames,callback) {

    if (containerNames && containerNames.length > 0 ) {

        async.each(containerNames, function(containerName, next){

                cdnAPI.cdnDisableContainer(containerName, function(containerDetails) {

                    //console.log('Disable container '+containerName);
                    next();
                    containerDetails = null;
                });
            },function(err) {

                callback(null,containerNames);
                err = null;
            }
        );
    }
    else {

        callback(null,containerNames);
    }
}

function deleteContainer (containerNames, callback) {

    if (containerNames && containerNames.length > 0 ) {

        async.each(containerNames, function(containerName, next) {

                cdnAPI.deleteContainers(containerName, function(containerDetails) {

                    //console.log('Delete '+containerName);
                    next();

                    if (containerDetails) {}
                });
            },function(err) {

                if(err) {callback(err);}
                else    {callback(null,containerNames);}
            }
        );
    }
    else {

        callback(null,containerNames);
    }
}

exports.cleanAllCloudFileAndCDNCatalogData = function cleanAllCloudFileAndCDNCatalogData (callback) {

    var cleanAllCloudFileAndCDNCatalogDataFlow = async.compose(deleteContainer,disableContainer,getAllContainer);

    cleanAllCloudFileAndCDNCatalogDataFlow(function (err, result) {

        if (err) {

            console.log(err);
            callback(null);
        }
        else if (!result)  {

            console.log('No container in CDN, no clean up needed');
            callback(null);
        }
        else if (result.length > 0) {

            console.log('Remove '+result.length+' containers data in CDN');
            callback(result);
        }
    });
};

function getEmptyContainer (callback) {

    cdnAPI.containerList(function (containerList) {

        var containerNames = [];

        if (containerList && containerList.length > 0) {

            async.each(containerList,function(containerInfo, next) {

                if (containerInfo.name.indexOf('_') !== -1) {

                    if (containerInfo.count === 0 || containerInfo.bytes === 0) {

                        containerNames.push(containerInfo.name);
                    }
                }
                next();

            },function(err) {

                if (err)    {callback(err);}
                else        {callback(null,containerNames);}
            });
        }
        else {callback(null,containerNames);}
    });
}

exports.removeEmptyCDNContainer = function removeEmptyCDNContainer (callback) {

    var removeEmptyCDNContainerFlow = async.compose(deleteContainer,disableContainer,getEmptyContainer);

    removeEmptyCDNContainerFlow(function (err, result) {

        if (err) {

            console.log(err);
            callback(null);
        }
        else if (!result)  {

            console.log('No empty container in CDN, no clean up needed');
            callback(null);
        }
        else if (result.length > 0) {

            console.log('Remove '+result.length+' empty containers in CDN');
            callback(result);
        }
    });
};