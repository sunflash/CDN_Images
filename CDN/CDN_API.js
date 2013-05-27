/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 27/05/13
 * Time: 13.49
 * To change this template use File | Settings | File Templates.
 */

var redis = require("redis"),
    client = redis.createClient();

var request = require('request');

var user = 'minreklame';
var id = '634ffdbbc9aff9c74a4c66818616384f';
var authLink = 'https://lon.identity.api.rackspacecloud.com/v1.0';

var authRedis = "cdn.auth";

exports.authenticate = function authenticate (callback) {

    request(
        {
            method:"GET",
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

                var authInfo = {};

                authInfo.authToken      = response.headers['x-auth-token'];
                authInfo.serverURL      = response.headers['x-server-management-url'];
                authInfo.storageURL     = response.headers['x-storage-url'];
                authInfo.cdnURL         = response.headers['x-cdn-management-url'];
                authInfo.storageToken   = response.headers['x-storage-token'];

                var now = new Date();
                now.setSeconds(now.getSeconds() + parseInt(response.headers['cache-control'].split("=")[1]));
                authInfo.expireDate = now;

                client.HMSET(authRedis,authInfo,function (err, obj) {
                    if (err) callback(err);
                    //else     console.dir(obj);

                    authInfo.expireInSec = response.headers['cache-control'].split("=")[1];
                    authInfo.expireDate  = now.toString();

                    callback(authInfo);
                    authInfo = null;
                    now = null;
                });
            }
            else {

                callback(null);
            }
        }
    );
}

