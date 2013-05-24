/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 21/05/13
 * Time: 10.57
 * To change this template use File | Settings | File Templates.
 */

var async = require("async");
var redis = require("redis"),
    client = redis.createClient();


exports.updateRedisData = function updateRedisData (activeCatalogs, activeCatalogsCount) {

    if (activeCatalogsCount > 0) {

        var  dataFlow  = async.compose(cleanUnusedExpiredData,overWriteWithFreshData);

        dataFlow(activeCatalogs, activeCatalogsCount,function (err, result) {

            if (err)                    console.log(err);
            else if (!result)           console.log('NO unusedExpiredCatalogs data in redis db, no clean up needed');
            else if (result.length > 0) console.log('Remove '+result.length+' unusedExpiredCatalogs data in redis db');
        });
    }

    console.log('***** RedisData *****');
}


var catalogRedisKeyPrefix = 'pub.';

function overWriteWithFreshData (activeCatalogsData, activeCatalogsCount, callback) {

    for (var i = 1; i <= activeCatalogsCount; i++) {

        var activeCatalogInfo = activeCatalogsData[i];
        var catalogPubID      = catalogRedisKeyPrefix + activeCatalogsData[i].pubID;

        client.HMSET(catalogPubID,activeCatalogInfo,function (err, obj) {
            if (err) callback(err);
            //else     console.dir(obj);
        });
    }

    callback(null,activeCatalogsData,activeCatalogsCount);
}


var pubKeysFilter = catalogRedisKeyPrefix + '*';

function cleanUnusedExpiredData(activeCatalogsData, activeCatalogsCount, callback) {

    client.keys(pubKeysFilter, function (err, replies) {

        if(err) callback(err);
        else
        {
            var activeCatalogsRedisDataKeys = [];
            for (var i = 1; i <= activeCatalogsCount; i++ ) {
                activeCatalogsRedisDataKeys.push(catalogRedisKeyPrefix+activeCatalogsData[i].pubID);
            }

            Array.prototype.diff = function(a) {
                return this.filter(function(i) {return !(a.indexOf(i) > -1);});
            };

            var unusedExpiredCatalogs =  replies.diff(activeCatalogsRedisDataKeys);

            if (unusedExpiredCatalogs.length > 0) {

                for (var i = 0; i < unusedExpiredCatalogs.length; i++) {

                    client.hdel(unusedExpiredCatalogs[i],"pubID","pubStart","pubStop","pageCount","iPaperID","iPaperLink",
                        function (err, obj) {
                            if (err) callback(err);
                        });
                }

                callback(null,unusedExpiredCatalogs);
            }
            else {callback(null);}
        }
    });
}

