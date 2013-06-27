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

var catalogRedisKeyPrefix = 'pub.';
var activeCatalogPubIDKey = 'activePubID';
var oldCatalogPubIDKey    = 'oldPubID';

function overWriteWithFreshData (activeCatalogsData, activeCatalogsCount, callback) {

    var activeCatalogPubID = [];

    for (var i = 1; i <= activeCatalogsCount; i++) {

        var activeCatalogInfo = activeCatalogsData[i];
        var catalogPubID      = catalogRedisKeyPrefix + activeCatalogsData[i].pubID;

        activeCatalogPubID.push(activeCatalogsData[i].pubID);

        client.HMSET(catalogPubID,activeCatalogInfo,function (err, obj) {
            if (err) {callback(err);}
            //else     console.dir(obj);
            if(obj) {}
        });
    }

    if (activeCatalogPubID.length > 0) {

        client.RENAME(activeCatalogPubIDKey, oldCatalogPubIDKey, function(err, obj) {

            obj = null;

            if (err) {callback(err);}
            else {

                client.SADD(activeCatalogPubIDKey, activeCatalogPubID, function (err, obj) {

                    if (err) {callback(err);}
                    else {
                        obj = null;
                        callback(null);
                    }
                });
            }
        });
    }
    else {

        callback('!! Empty activeCatalogsData');
    }
}

var cdnCleanKey = 'cdn.clean';

function cleanUnusedExpiredData(callback) {

    client.SDIFF(oldCatalogPubIDKey, activeCatalogPubIDKey, function (err, unusedExpiredCatalogs) {

        if(err) {callback(err);}
        else if (unusedExpiredCatalogs.length > 0)
        {
            for (var i = 0; i < unusedExpiredCatalogs.length; i++) {

                var unusedExpiredCatalogKey = catalogRedisKeyPrefix + unusedExpiredCatalogs[i];

                client.DEL(unusedExpiredCatalogKey, function (err, obj) {
                        if (err) {callback(err);}
                        obj = null;
                    }
                );
            }

            client.SADD(cdnCleanKey, unusedExpiredCatalogs, function (err, obj) {

                if(err) {callback(err);}
                obj = null;
                callback(null,unusedExpiredCatalogs);
            });
        }
        else {callback(null, null);}

        client.DEL(oldCatalogPubIDKey, function(err, obj) {
            if(err) {callback(err);}
            obj = null;
        });
    });
}

var  dataFlow  = async.compose(cleanUnusedExpiredData,overWriteWithFreshData);

exports.updateRedisData = function updateRedisData (activeCatalogs, activeCatalogsCount) {

    if (activeCatalogsCount > 0) {

        dataFlow(activeCatalogs, activeCatalogsCount,function (err, result) {

            if (err)                    {console.log(err);}
            else if (!result)           {console.log('NO unusedExpiredCatalogs data in redis db, no clean up needed');}
            else if (result.length > 0) {
                console.log('Remove '+result.length+' unusedExpiredCatalogs data in redis db');
                result = null;
            }
        });
    }

    console.log('***** RedisData *****');
};

