/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 27/05/13
 * Time: 13.49
 * To change this template use File | Settings | File Templates.
 */

var request = require('request');
var redis = require("redis"),
    client = redis.createClient();

var user = 'minreklame';
var id = '634ffdbbc9aff9c74a4c66818616384f';
var authLink = 'https://lon.identity.api.rackspacecloud.com/v1.0';

//--------------------------------------------------------------------------------

// Export Module functions

exports.authDetails = function authDetails (callback) {

    getAuthInfo(function (api) {
        callback(api);
    });
}

exports.accountDetails = function accountDetails (callback) {

    getAccountDetails(function (accountDetails) {
        callback(accountDetails);
    });
}

exports.containerDetails = function containerDetails (containerName,callback) {

    if (containerName && containerName.length > 0) {

        encodeContainerName(containerName, function (encodedContainerName) {

            getContainerDetails(encodedContainerName,function(containerDetails){
                callback(containerDetails);
            });
        });
    }
    else callback(null);
}

exports.containerList = function containerList (callback) {

    getContainerList(null,function(containerList){
        callback(containerList);
    });
}

exports.createContainer = function createContainer (containerName, metaData, callback) {

    if (containerName && containerName.length > 0) {

        encodeContainerName(containerName, function (encodedContainerName) {

            createCloudFileContainer(encodedContainerName, metaData, function (statusCode) {
                callback(statusCode);
            });
        });
    }
    else callback(null);
}

exports.setUpdateDeleteContainerMetaData = function setUpdateDeleteContainerMetaDat (containerName, metaData, callback) {

    if (metaData && containerName && containerName.length > 0) {

        encodeContainerName(containerName, function (encodedContainerName) {

            setUpdateDeleteCloudFileContainerMetaData(encodedContainerName, metaData, function (statusCode) {
                callback(statusCode);
            });
        });
    }
    else callback(null);
}

exports.getContainerObjects = function getContainerObjects (containerName, callback) {

    if (containerName && containerName.length > 0) {

        encodeContainerName(containerName, function (encodedContainerName) {

            getCloudFileContainerObjects(encodedContainerName,null,function (objectsList) {
                callback(objectsList);
            });
        });
    }
    else callback(null);
}

exports.deleteSingleObject = function deleteSingleObject (containerName, objectName, callback) {

    if (containerName && objectName) {

        if(containerName.length > 0 && objectName.length > 0) {

        }
        else callback(null);
    }
    else callback(null);
}

//--------------------------------------------------------------------------------

// URL encode container and objects name, cut to under 256 byte string and replace '/' with '_'

function encodeContainerName (containerName, callback) {

    if (containerName) {

        if (containerName.length > 0) {

            containerName = containerName.replace(/\//g,'_');
            containerName = encodeURIComponent(containerName);

            if (containerName.length > 250) {
                containerName = containerName.substr(0,250);
            }

            callback(containerName);
        }
        else callback(null);
    }
    else callback(null);
}

//--------------------------------------------------------------------------------

// Authentication to rackspace CDN

var authInfo = {};
var authRedis = 'cdn.auth';

function authenticate (callback) {

    request(
        {
            method:'GET',
            uri:authLink,
            headers:{
                'X-Auth-User':user,
                'X-Auth-Key':id
            }
        }
        , function (error, response, body) {

            //console.log(response.statusCode);
            //console.log(body);
            //console.log(response.headers);

            if (response.statusCode == 204 || response.statusCode == 202) {

                authInfo.authToken      = response.headers['x-auth-token'];
                authInfo.serverURL      = response.headers['x-server-management-url'];
                authInfo.storageURL     = response.headers['x-storage-url'];
                authInfo.cdnURL         = response.headers['x-cdn-management-url'];
                authInfo.storageToken   = response.headers['x-storage-token'];

                var now = new Date();
                now.setSeconds(now.getSeconds() + parseInt(response.headers['cache-control'].split("=")[1]));
                authInfo.expireDate = now;

                //console.log(authInfo);
                //console.log('ExpireInSec '+response.headers['cache-control'].split("=")[1]);

                client.HMSET(authRedis,authInfo,function (err, obj) {

                    if (err) callback(err);
                    //else     console.dir(obj);

                    callback(authInfo);
                    now = null;
                });
            }
            else {callback(null);}
        }
    );
}

function getAuthInfo (callback) {

    if (!authInfo.authToken) {

        client.keys(authRedis, function (err, replies) {

            if (err) callback(err,null);
            else
            {
                if (replies.length == 0) {

                    authenticate(function(authInfoFresh){

                        //console.log('Fresh authInfo from rackspace');
                        callback(authInfoFresh);
                    });
                }
                else if  (replies.length == 1) {

                    getAuthInfoFromRedis (function (authInfoRedis) {

                        if (authInfoRedis) {

                            //console.log('Loading authInfo from local redis db');
                            authInfo = authInfoRedis;
                            callback(authInfoRedis);
                        }
                    });
                }
            }
        });
    }
    else {

        //console.log('Reuse authInfo from global variable');
        callback(authInfo);
    }
}

function getAuthInfoFromRedis(callback) {

    client.hgetall(authRedis, function (err, obj) {

        if (err)        callback(null);
        else if (obj)   callback(obj);
        else            callback(null);
    });
}

//--------------------------------------------------------------------------------

// Get Cloud Files account details

function getAccountDetails (callback) {

    getAuthInfo(function (api) {

        request(
            {
                method:'HEAD',
                uri:api.storageURL,
                headers:{
                    'X-Auth-Token':api.authToken
                }
            }
            , function (error, response, body) {

                if (response.statusCode == 204) {

                    var accountDetails = {};
                    accountDetails.containerCount = response.headers['x-account-container-count'];
                    accountDetails.objectCount    = response.headers['x-account-object-count']
                    accountDetails.bytesUsed      = response.headers['x-account-bytes-used'];

                    callback(accountDetails);
                    accountDetails = null;
                }
                else if (response.statusCode == 401) {

                    authenticate(function(authInfoFresh) {

                        request(
                            {
                                method:'HEAD',
                                uri:api.storageURL,
                                headers:{
                                    'X-Auth-Token':authInfoFresh.authToken
                                }
                            }
                            , function (error, response, body) {

                                if (response.statusCode == 204) {

                                    var accountDetails = {};
                                    accountDetails.containerCount = response.headers['x-account-container-count'];
                                    accountDetails.objectCount    = response.headers['x-account-object-count']
                                    accountDetails.bytesUsed      = response.headers['x-account-bytes-used'];

                                    callback(accountDetails);
                                    accountDetails = null;
                                }
                                else callback(null);
                            }
                        );
                    });
                }
                else callback(null);
            }
        );

    });
}

//--------------------------------------------------------------------------------

// Get list of container in json, max 10000 containers for each fetch

function getContainerList (maxContainersFetchLimit, callback) {

    if (!maxContainersFetchLimit || maxContainersFetchLimit > 10000) maxContainersFetchLimit = 10000;

    getAccountDetails(function (accountDetails) {

        if (accountDetails) {

            var containersCount = parseInt(accountDetails.containerCount);
            var maxRound     = Math.ceil(containersCount/maxContainersFetchLimit);

           getContainerListInChunk(maxContainersFetchLimit,maxRound,null,null,null,function(containerObjects) {

                callback(containerObjects);

                containersCount = null;
                maxRound        = null;
            });
        }
        else callback(null);
    });
}

function getContainerListInChunk (maxContainersFetchLimit,maxRound,round,marker,containers,callback) {

    if (!maxContainersFetchLimit || maxContainersFetchLimit > 10000) maxContainersFetchLimit = 10000;
    if (!containers) containers = [];
    if (!round)      round  = 0;

    if (round < maxRound) {

        getAuthInfo(function (api) {

            var url = api.storageURL+'?format=json'+'&limit='+maxContainersFetchLimit;
            if (marker) url = url + '&marker=' + marker;

            request(
                {
                    method:'GET',
                    uri:url,
                    headers:{
                        'X-Auth-Token':api.authToken
                    }
                }
                , function (error, response, body) {

                    if (response.statusCode == 200) {

                        var containerArray = JSON.parse(body);

                        if (containerArray.length > 0) {

                            containers = containers.concat(containerArray);

                            round++;
                            if (round < maxRound) {
                                marker = null;
                                var lastContainerName = containerArray[containerArray.length-1].name;
                                getContainerListInChunk (maxContainersFetchLimit,maxRound,round,lastContainerName,containers,callback);
                            }
                            else if (round == maxRound) {
                                callback(containers);
                                marker = null;
                            }
                        }
                        else callback(null);

                        containerArray  = null;
                        url = null;
                    }
                    else if (response.statusCode == 204) {

                        callback(null);
                        url = null;
                    }
                    else if (response.statusCode == 401) {

                        authenticate(function(authInfoFresh) {

                            request(
                                {
                                    method:'GET',
                                    uri:url,
                                    headers:{
                                        'X-Auth-Token':authInfoFresh.authToken
                                    }
                                }
                                , function (error, response, body) {

                                    if (response.statusCode == 200) {

                                        var containerArray = JSON.parse(body);

                                        if (containerArray.length > 0) {

                                            containers = containers.concat(containerArray);

                                            round++;
                                            if (round < maxRound) {
                                                marker = null;
                                                var lastContainerName = containerArray[containerArray.length-1].name;
                                                getContainerListInChunk (maxContainersFetchLimit,maxRound,round,lastContainerName,containers,callback);
                                            }
                                            else if (round == maxRound) {
                                                callback(containers);
                                                marker = null;
                                            }
                                        }
                                        else callback(null);

                                        containerArray  = null;
                                        url = null;
                                    }
                                    else callback(null);
                                }
                            );
                        });
                    }
                    else callback(null);
                }
            );

        });
    }
    else callback(null);
}

//--------------------------------------------------------------------------------

// Get Container details

function getContainerDetails (containerName, callback) {

    getAuthInfo(function (api) {

        request(
            {
                method:'HEAD',
                uri:api.storageURL+'/'+containerName+'?format=json',
                headers:{
                    'X-Auth-Token':api.authToken
                }
            }
            , function (error, response, body) {

                if (response.statusCode == 204) {

                    var containerDetails = {};
                    containerDetails.objectsCount = response.headers['x-container-object-count'];
                    containerDetails.bytesUsed    = response.headers['x-container-bytes-used'];

                    var metaTag;
                    var metaHeaderPrefix = 'x-container-meta-';
                    var log = 'x-container-meta-access-log-delivery';

                    for (x in response.headers) {

                        if (x.indexOf(metaHeaderPrefix) != -1 && x != log) {

                            if (!metaTag) metaTag = {};
                            metaTag[x.substr(x.indexOf(metaHeaderPrefix)+metaHeaderPrefix.length, x.length-metaHeaderPrefix.length)] = response.headers[x];
                        }
                    }

                    if (metaTag) containerDetails['metaTag'] = metaTag;

                    callback(containerDetails);
                    containerDetails = null;
                    metaTag = null;
                    metaHeaderPrefix = null;
                    log = null;
                }
                else if (response.statusCode == 401) {

                    authenticate(function(authInfoFresh) {

                        request(
                            {
                                method:'HEAD',
                                uri:api.storageURL+'/'+containerName+'?format=json',
                                headers:{
                                    'X-Auth-Token':authInfoFresh.authToken
                                }
                            }
                            , function (error, response, body) {

                                if (response.statusCode == 204) {

                                    var containerDetails = {};
                                    containerDetails.objectsCount = response.headers['x-container-object-count'];
                                    containerDetails.bytesUsed    = response.headers['x-container-bytes-used'];

                                    var metaTag;
                                    var metaHeaderPrefix = 'x-container-meta-';
                                    var log = 'x-container-meta-access-log-delivery';

                                    for (x in response.headers) {

                                        if (x.indexOf(metaHeaderPrefix) != -1 && x != log) {

                                            if (!metaTag) metaTag = {};
                                            metaTag[x.substr(x.indexOf(metaHeaderPrefix)+metaHeaderPrefix.length, x.length-metaHeaderPrefix.length)] = response.headers[x];
                                        }
                                    }

                                    if (metaTag) containerDetails['metaTag'] = metaTag;

                                    callback(containerDetails);
                                    containerDetails = null;
                                    metaTag = null;
                                    metaHeaderPrefix = null;
                                    log = null;
                                }
                                else callback(null);
                            }
                        );
                    });
                }
                else callback(null);
            }
        );

    });
}

//--------------------------------------------------------------------------------

// Create new container

/* NOTICE :
 you can have 4096 bytes maximum overall metadata, with 90 distinct metadata items at the most.
 Each may have a 128 character name length with a 256 max value length each.
 Any valid UTF-8 http header value is allowed for metadata.
 */

function createCloudFileContainer (containerName, metaData, callback) {

    getAuthInfo(function (api) {

        var headerValues = {};
        if (metaData) headerValues = metaData;
        headerValues['X-Auth-Token'] = api.authToken;

        request(
            {
                method:'PUT',
                uri:api.storageURL+'/'+containerName,
                headers:headerValues
            }
            , function (error, response, body) {

                if (response.statusCode == 201) {

                    callback(1);
                }
                else if (response.statusCode == 202) {

                    callback(2);
                }
                else if (response.statusCode == 401) {

                    authenticate(function(authInfoFresh) {

                        headerValues['X-Auth-Token'] = authInfoFresh.authToken;

                        request(
                            {
                                method:'PUT',
                                uri:api.storageURL+'/'+containerName,
                                headers:headerValues
                            }
                            , function (error, response, body) {

                                if (response.statusCode == 201) {

                                    callback(1);
                                }
                                else if (response.statusCode == 202) {

                                    callback(2);
                                }
                                else callback(null);
                            }
                        );
                    });
                }
                else callback(null);

                headerValues = null;
            }
        );
    });
}

//--------------------------------------------------------------------------------

// Set update and delete container meta data
// Set, update : X-Container-Meta-Book: 'Hello world'
// Delete      : X-Remove-Container-Meta-Name: foo

function setUpdateDeleteCloudFileContainerMetaData (containerName, metaData, callback) {

    getAuthInfo(function (api) {

        var headerValues = {};
        if (metaData) headerValues = metaData;
        headerValues['X-Auth-Token'] = api.authToken;

        request(
            {
                method:'POST',
                uri:api.storageURL+'/'+containerName,
                headers:headerValues
            }
            , function (error, response, body) {

                if (response.statusCode == 204) {
                    callback(1);
                }
                else if (response.statusCode == 404) {
                    callback(null);
                }
                else if (response.statusCode == 401) {

                    authenticate(function(authInfoFresh) {

                        headerValues['X-Auth-Token'] = authInfoFresh.authToken;

                        request(
                            {
                                method:'POST',
                                uri:api.storageURL+'/'+containerName,
                                headers:headerValues
                            }
                            , function (error, response, body) {

                                if (response.statusCode == 204) {
                                    callback(1);
                                }
                                else if (response.statusCode == 404) {
                                    callback(null);
                                }
                                else callback(null);
                            }
                        );
                    });
                }
                else callback(null);

                headerValues = null;
            }
        );
    });
}

//--------------------------------------------------------------------------------

// List objects in container, max 10000 objects for each fetch

function getCloudFileContainerObjects (containerName, maxObjectsFetchLimit, callback) {

    if (!maxObjectsFetchLimit || maxObjectsFetchLimit > 10000) maxObjectsFetchLimit = 10000;

    getContainerDetails(containerName, function (containerDetails) {

        if (containerDetails) {

            var objectsCount = parseInt(containerDetails.objectsCount);
            var maxRound     = Math.ceil(objectsCount/maxObjectsFetchLimit);

            getCloudFileContainerObjectsInChunk(containerName,maxObjectsFetchLimit,maxRound,null,null,null,function(containerObjects) {

                callback(containerObjects);

                objectsCount = null;
                maxRound     = null;
            });
        }
        else callback(null);
    });
}

function getCloudFileContainerObjectsInChunk (containerName,maxObjectsFetchLimit,maxRound,round,marker,objects,callback) {

    if (!maxObjectsFetchLimit || maxObjectsFetchLimit > 10000) maxObjectsFetchLimit = 10000;
    if (!objects) objects = [];
    if (!round)   round  = 0;

    if (round < maxRound) {

        getAuthInfo(function (api) {

            var url = api.storageURL+'/'+containerName+'?format=json'+'&limit='+maxObjectsFetchLimit;
            if (marker) url = url + '&marker=' + marker;

            request(
                {
                    method:'GET',
                    uri:url,
                    headers:{
                        'X-Auth-Token': api.authToken
                    }
                }
                , function (error, response, body) {

                    if (response.statusCode == 200) {

                        var objectsArray =  JSON.parse(body);

                        if (objectsArray.length > 0) {

                            objects = objects.concat(objectsArray);

                            round++;
                            if (round < maxRound) {
                                marker = null;
                                var lastObjectName = objectsArray[objectsArray.length-1].name;
                                getCloudFileContainerObjectsInChunk(containerName,maxObjectsFetchLimit,maxRound,round,lastObjectName,objects,callback);
                            }
                            else if (round == maxRound) {
                                callback(objects);
                                marker = null;
                            }
                        }
                        else callback(null);

                        objectsArray  = null;
                        url = null;
                    }
                    else if (response.statusCode == 204) {

                        //console.log('NO objects in container');
                        callback(null);
                        url = null;
                    }
                    else if (response.statusCode == 404) {

                        //console.log('container not exist');
                        callback(null);
                        url = null;
                    }
                    else if (response.statusCode == 401) {

                        authenticate(function(authInfoFresh) {

                            request(
                                {
                                    method:'GET',
                                    uri:url,
                                    headers:{
                                        'X-Auth-Token': authInfoFresh.authToken
                                    }
                                }
                                , function (error, response, body) {

                                    if (response.statusCode == 200) {

                                        var objectsArray =  JSON.parse(body);

                                        if (objectsArray.length > 0) {

                                            objects = objects.concat(objectsArray);

                                            round++;
                                            if (round < maxRound) {
                                                marker = null;
                                                var lastObjectName = objectsArray[objectsArray.length-1].name;
                                                getCloudFileContainerObjectsInChunk(containerName,maxObjectsFetchLimit,maxRound,round,lastObjectName,objects,callback);
                                            }
                                            else if (round == maxRound) {
                                                callback(objects);
                                                marker = null;
                                            }
                                        }
                                        else callback(null);

                                        objectsArray  = null;
                                        url = null;
                                    }
                                    else if (response.statusCode == 204) {

                                        //console.log('NO objects in container');
                                        callback(null);
                                        url = null;
                                    }
                                    else if (response.statusCode == 404) {

                                        //console.log('container not exist');
                                        callback(null);
                                        url = null;
                                    }
                                    else callback(null);
                                }
                            );
                        });
                    }
                    else callback(null);
                }
            );
        });
    }
    else callback(null);
}

//--------------------------------------------------------------------------------

// Delete single object, one at a time
