/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 17/05/13
 * Time: 13.32
 * To change this template use File | Settings | File Templates.
 */

var catalogMetaData = require('./Data/CatalogMetaData');

var cronJob = require('cron').CronJob;

exports.cron = function cron () {

    new cronJob('5 */5 * * * *', function(){

        var currentTime = new Date();
        console.log(currentTime);

        catalogMetaData.getCatalogData(
            function (data) {

            });
    },null, true);
}

