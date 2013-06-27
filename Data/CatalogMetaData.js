/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 10/05/13
 * Time: 15.01
 * To change this template use File | Settings | File Templates.
 */

var mysql = require('mysql');
var async = require('async');
var downloadImages = require('./DownloadImages');
var redisData      = require('./RedisData');

var pool = mysql.createPool({
    host: 'minreklame.webhotel.net',
    user: 'mroffice',
    password: 'Pb9h9RY3',
    database: 'minreklame_db'
    }
);

var activeCatalogQuery = "SELECT id,time_start,time_stop,path,ipaper_id FROM mr_stories_external ";
activeCatalogQuery += "WHERE active = 1 AND time_stop > NOW() AND time_start < NOW() ";
activeCatalogQuery += "AND time_stop < DATE_ADD(NOW(), INTERVAL 1 YEAR) AND time_start > DATE_SUB(NOW(), INTERVAL 1 YEAR) ";
activeCatalogQuery += "AND path LIKE  '%minreklamedk.ipapercms.dk%'";

var catalogPageCountQuery = "SELECT COUNT(*) AS pageCount FROM mr_story_pages WHERE id = ?";

exports.getCatalogData = function getCatalogData (callBack) {

    //console.log(activeCatalogQuery);

    async.waterfall([

        function (callback) {

        // Get active catalogs info from admin server

            pool.getConnection(function(err, connection) {

                connection.query(activeCatalogQuery, function(err, rows) {

                    connection.end();

                    if (err) {
                        //console.log(err.code);
                        //console.log(err.fatal);
                        callback('Connection error');
                    }
                    else {

                        if (rows.length > 0) {callback(null,rows);}
                        else                 {callback('Error, NO active catalogs');}
                    }
                });
            });
        },
        function (activeCatalogArray, callback) {

        // Get active catalogs pageCounts from admin server

            async.times(activeCatalogArray.length, function(n, next){

                var iPaperID = activeCatalogArray[n].ipaper_id;

                pool.getConnection(function(err, connection) {

                    connection.query(catalogPageCountQuery,iPaperID,function(err, rows) {
                        connection.end();

                        if (err) {
                            //console.log(err.code);
                            //console.log(err.fatal);
                            next('Connection error');
                        }
                        else {next(null,rows);}

                    });
                });

            }, function(err, pageCounts) {

                if (err) {callback(err);}
                else {

                    for (var i = 0; i < activeCatalogArray.length; i++) {
                        activeCatalogArray[i].pageCount = pageCounts[i][0].pageCount;
                    }
                    callback(null,activeCatalogArray);
                }
            });
        }

    ],function(err,activeCatalogArray) {

    // Parse data and do something with data

        if (err) {
            console.log(err);
            callBack(null);
        }
        else
        {
            var activeCatalogs = {};

            if (activeCatalogArray.length > 0) {

                for (var i = 1; i <= activeCatalogArray.length; i++) {

                    var catalog = {};
                    var pageCount = activeCatalogArray[i-1].pageCount;
                    var pubID     = activeCatalogArray[i-1].id;

                    catalog.pubID    = pubID;
                    catalog.pubStart = activeCatalogArray[i-1].time_start;
                    catalog.pubStop  = activeCatalogArray[i-1].time_stop;
                    catalog.pageCount   = pageCount;
                    catalog.iPaperID    = activeCatalogArray[i-1].ipaper_id;
                    catalog.iPaperLink  = activeCatalogArray[i-1].path;

                    activeCatalogs[i] = catalog;

                    if (pageCount === 0) {
                        console.log("!!! Error, catalog "+pubID+ " have NO pages.");
                    }
                    else if ( (pageCount%2) === 1) {

                        //console.log("!!! catalog "+pubID+ " have odd pages "+pageCount);
                    }
                    //else {console.log("Catalog "+pubID+" have "+pageCount+" pages.");}

                }

                downloadImages.downloadActiveCatalogImage(activeCatalogs,activeCatalogArray.length);
                redisData.updateRedisData(activeCatalogs,activeCatalogArray.length);
            }

            callBack (activeCatalogs);
        }
        activeCatalogArray = null;
    });


    console.log('------------------------------------------------------------------');
    console.log('***** CatalogMetaData *****');
};





