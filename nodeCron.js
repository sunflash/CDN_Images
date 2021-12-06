/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 17/05/13
 * Time: 13.32
 * To change this template use File | Settings | File Templates.
 */

var catalogMetaData = require("./Data/CatalogMetaData");
var cdnClean = require("./CDN/CDN_Clean");

var cronJob = require("cron").CronJob;

exports.cron = function cron() {
  var getCatalogDataCronJob = new cronJob(
    "5 */5 * * * *",
    function () {
      var currentTime = new Date();
      console.log(currentTime);

      catalogMetaData.getCatalogData(function () {});
    },
    null,
    false
  );

  getCatalogDataCronJob.start();

  var cleanExpiredDataCronJob = new cronJob(
    "0 30 3 * * *",
    function () {
      var currentTime = new Date();
      console.log(currentTime);

      cdnClean.cleanExpiredDataInCloudFileAndCDN(function () {});
    },
    null,
    false
  );

  cleanExpiredDataCronJob.start();

  var removeEmptyContainerCronJob = new cronJob(
    "0 30 4 * * *",
    function () {
      var currentTime = new Date();
      console.log(currentTime);

      cdnClean.removeEmptyCDNContainer(function () {});
    },
    null,
    false
  );

  removeEmptyContainerCronJob.start();
};
