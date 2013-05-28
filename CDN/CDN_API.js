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

exports.getAuthInfo =  function getAuthInfo (callback) {

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

exports.accountDetails = function accountDetails (callback) {


}


