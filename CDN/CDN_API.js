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
    })
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
                else if (response.statusCode == 401) {

                    authenticate(function(authInfoFresh) {

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