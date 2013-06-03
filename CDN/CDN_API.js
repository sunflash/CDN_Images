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

exports.authDetails = function authDetails (callback) {

    getAuthInfo(function (api) {
        callback(api);
    });
}

//--------------------------------------------------------------------------------

// Get Cloud Files account details

exports.accountDetails = function accountDetails (callback) {

    getAccountDetails(function (accountDetails) {
       callback(accountDetails);
    });
}

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

// Get list of container in json, max 10000

exports.containerList = function containerList (callback) {

    getContainerList(function(containerList){
        callback(containerList);
    });
}

function getContainerList (callback) {

    getAuthInfo(function (api) {

        request(
            {
                method:'GET',
                uri:api.storageURL+'?format=json',
                headers:{
                    'X-Auth-Token':api.authToken
                }
            }
            , function (error, response, body) {

                if (response.statusCode == 200) {

                    var containerArray =  JSON.parse(body);
                    var containers = {};

                    for (var i = 0; i < containerArray.length; i++) {

                        var container = {};
                        container.containerName  = containerArray[i].name;
                        container.objectsCount = containerArray[i].count;
                        container.containerBytes = containerArray[i].bytes;
                        containers[i+1]= container;
                        container = null;
                    }

                    callback(containers);
                    containerArray = null;
                    containers = null;
                }
                else if (response.statusCode == 204) {

                    callback(null);
                }
                else if (response.statusCode == 401) {

                    authenticate(function(authInfoFresh) {

                        request(
                            {
                                method:'GET',
                                uri:api.storageURL+'?format=json',
                                headers:{
                                    'X-Auth-Token':authInfoFresh.authToken
                                }
                            }
                            , function (error, response, body) {

                                if (response.statusCode == 200) {

                                    var containerArray =  JSON.parse(body);
                                    var containers = {};

                                    for (var i = 0; i < containerArray.length; i++) {

                                        var container = {};
                                        container.containerName  = containerArray[i].name;
                                        container.objectsCount = containerArray[i].count;
                                        container.containerBytes = containerArray[i].bytes;
                                        containers[i+1]= container;
                                        container = null;
                                    }

                                    callback(containers);
                                    containerArray = null;
                                    containers = null;
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

// Get Container details

exports.containerDetails = function containerDetails (containerName,callback) {

    encodeContainerName(containerName, function (encodedContainerName) {

        getContainerDetails(encodedContainerName,function(containerDetails){
            callback(containerDetails);
        });
    });
}

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

exports.createContainer = function createContainer (containerName, metaData, callback) {

    encodeContainerName(containerName, function (encodedContainerName) {

        createCloudFileContainer(encodedContainerName, metaData, function (statusCode) {
            callback(statusCode);
        });
    });
}

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