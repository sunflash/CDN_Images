/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 18/06/13
 * Time: 10.39
 * To change this template use File | Settings | File Templates.
 */

var fs = require("fs");
var async = require("async");
var path = require("path");
var redis = require("redis"),
  client = redis.createClient();

var cdnAPI = require("./CDN_API");
var fileSystem = require("../Data/FileSystem");
var resizeImage = require("../ImageIO/ResizeImage");

var cdnRedisKeyPrefix = "CDN.";

var catalogRedisKeyPrefix = "pub.";
var imageSuffix = "Image.ashx?ImageType=Zoom&PageNumber=";
var saveFilePathPrefix = "../images";

function downloadImages(parameters, res, callback) {
  client.HGETALL(
    catalogRedisKeyPrefix + parameters.PublicationID,
    function (err, obj) {
      if (err) {
        callback(err);
      } else if (obj) {
        if (
          parseInt(parameters.PageNumber, 10) <= parseInt(obj.pageCount, 10)
        ) {
          var savePath = path.join(
            saveFilePathPrefix,
            parameters.PublicationID,
            parameters.PageNumber + ".jpg"
          );

          fs.exists(savePath, function (exists) {
            if (exists) {
              callback(null, parameters, res, obj);
            } else {
              var folderPath = path.join(
                saveFilePathPrefix,
                parameters.PublicationID
              );
              var url = obj.iPaperLink + imageSuffix + parameters.PageNumber;

              fs.exists(folderPath, function (exists) {
                if (exists) {
                  fileSystem.downloadFileFromURL(
                    url,
                    savePath,
                    function (err, success) {
                      if (err) {
                        callback(err);
                      } else if (success) {
                        callback(null, parameters, res, obj);
                      }
                    }
                  );
                } else {
                  fs.mkdir(folderPath, function (error) {
                    if (error) {
                      callback(error);
                    } else {
                      fileSystem.downloadFileFromURL(
                        url,
                        savePath,
                        function (err, success) {
                          if (err) {
                            callback(err);
                          } else if (success) {
                            callback(null, parameters, res, obj);
                          }
                        }
                      );
                    }
                  });
                }
              });
            }
          });
        } else {
          callback(
            "!! Page " +
              parameters.PageNumber +
              " not exist in " +
              parameters.PublicationID
          );
        }
      } else if (!obj) {
        callback(
          "!! Publication " + parameters.PublicationID + " is not exist"
        );
      }
    }
  );
}

function resizeImageAndServeLocalResizeImage(
  parameters,
  res,
  publicationInfo,
  callback
) {
  var filename = parameters.PageNumber + ".jpg";
  var saveFilePath = path.join(
    saveFilePathPrefix,
    parameters.PublicationID.toString(),
    parameters.Width + "x" + parameters.Height,
    filename
  );

  fs.exists(saveFilePath, function (exists) {
    if (exists) {
      fs.stat(saveFilePath, function (err, stats) {
        if (err || (stats && stats.size === 0)) {
          fs.unlink(saveFilePath, function (err) {
            if (err) {
              //console.log(err);
            }

            resizeImage.resizeImage(parameters, function (resizeFilePath) {
              if (!resizeFilePath) {
                res.send(404, "Aaaa ooo!");
                res.end();
                callback("!! NO resize image " + resizeFilePath);
              } else {
                res.sendfile(path.resolve(resizeFilePath));
                callback(null, parameters, resizeFilePath, publicationInfo);
              }
            });
          });
        } else {
          res.sendfile(path.resolve(saveFilePath));
          callback(null, parameters, saveFilePath, publicationInfo);
        }
      });
    } else {
      resizeImage.resizeImage(parameters, function (resizeFilePath) {
        if (!resizeFilePath) {
          res.send(404, "Aaaa ooo!");
          res.end();
          callback("!! NO resize image " + resizeFilePath);
        } else {
          res.sendfile(path.resolve(resizeFilePath));
          callback(null, parameters, resizeFilePath, publicationInfo);
        }
      });
    }
  });
}

function uploadImageToCloudFile(
  parameters,
  resizeImageFilePath,
  publicationInfo,
  callback
) {
  // Set, update : X-Object-Meta-Book: 'Hello world'
  // Delete      : X-Remove-Object-Meta-Name: foo

  var publicationID = parameters.PublicationID;

  var containerName =
    publicationID + "_" + parameters.Width + "x" + parameters.Height;

  var metaData = { "X-Object-Meta-ID": publicationID.toString() };
  var date = new Date(publicationInfo.pubStop);

  var contentType = "image/jpeg";

  cdnAPI.objectDetails(
    containerName,
    path.basename(resizeImageFilePath),
    function (data) {
      var emptyFlag = 0;

      if (data && data.contentSize === "0") {
        emptyFlag = 1;
      }

      if (!data || emptyFlag === 1) {
        cdnAPI.createUpdateObject(
          resizeImageFilePath,
          containerName,
          contentType,
          metaData,
          date,
          function (data) {
            if (data) {
              callback(null, parameters, containerName, resizeImageFilePath);
            } else {
              callback(
                "!! Failed to create /" +
                  publicationID +
                  "/" +
                  path.basename(resizeImageFilePath)
              );
            }

            resizeImageFilePath = null;
            publicationInfo = null;
          }
        );
      } else {
        callback(null, parameters, containerName, resizeImageFilePath);
      }
    }
  );
}

function verifyImageInCloudFile(
  parameters,
  containerName,
  resizeImageFilePath,
  callback
) {
  cdnAPI.objectDetails(
    containerName,
    path.basename(resizeImageFilePath),
    function (data) {
      var emptyFlag = 0;

      if (data && data.contentSize === "0") {
        emptyFlag = 1;
      }

      if (!data || emptyFlag === 1) {
        callback(
          "!! Empty file " +
            containerName +
            "/" +
            parameters.PageNumber +
            ".jpg"
        );
      } else {
        callback(null, parameters, containerName);
      }
    }
  );
}

function enableContainerForCDN(parameters, containerName, callback) {
  var key =
    cdnRedisKeyPrefix +
    parameters.PublicationID +
    "." +
    parameters.Width +
    "x" +
    parameters.Height;

  client.HGETALL(key, function (err, obj) {
    if (err) {
      parameters = null;
      containerName = null;

      callback(err);
    } else if (!obj) {
      //console.log('No key');

      cdnAPI.cdnEnableContainer(
        containerName,
        900,
        function (cdnEnabledContainerDetails) {
          if (cdnEnabledContainerDetails) {
            var cdnURLInfo = {};

            cdnURLInfo.cdnURL = cdnEnabledContainerDetails["x-cdn-uri"];
            cdnURLInfo.cdnSSL = cdnEnabledContainerDetails["x-cdn-ssl-uri"];
            cdnURLInfo.cdniOS = cdnEnabledContainerDetails["x-cdn-ios-uri"];
            cdnURLInfo.cdnStream =
              cdnEnabledContainerDetails["x-cdn-streaming-uri"];

            cdnURLInfo[parameters.PageNumber.toString()] = "jpg";

            client.HMSET(key, cdnURLInfo, function (err, result) {
              if (err) {
                callback(err);
              } else {
                callback(
                  null,
                  cdnURLInfo.cdnURL + "/" + parameters.PageNumber + ".jpg"
                );
              }

              parameters = null;
              containerName = null;

              cdnEnabledContainerDetails = null;
              result = null;
            });
          } else {
            callback("Enable container " + containerName + " failed");
          }
        }
      );
    } else if (obj) {
      //console.log('Key exist');

      obj[parameters.PageNumber.toString()] = "jpg";

      client.HMSET(key, obj, function (err, result) {
        if (err) {
          callback(err);
        } else {
          callback(null, obj.cdnURL + "/" + parameters.PageNumber + ".jpg");
        }

        parameters = null;
        containerName = null;

        obj = null;
        result = null;
      });
    }
  });
}

var cdnImageFlow = async.compose(
  enableContainerForCDN,
  verifyImageInCloudFile,
  uploadImageToCloudFile,
  resizeImageAndServeLocalResizeImage,
  downloadImages
);

exports.cdnImage = function cdnImage(parameters, res, callback) {
  if (
    parameters.PublicationID &&
    parameters.Width &&
    parameters.Height &&
    parameters.PageNumber
  ) {
    var key =
      cdnRedisKeyPrefix +
      parameters.PublicationID +
      "." +
      parameters.Width +
      "x" +
      parameters.Height;

    client.HGETALL(key, function (err, obj) {
      if (err) {
        res.end();
        callback(null);
        parameters = null;
      } else if (obj && obj[parameters.PageNumber.toString()]) {
        //console.log('Use CDN image '+parameters['PageNumber']);

        var url =
          obj.cdnURL +
          "/" +
          parameters.PageNumber +
          "." +
          obj[parameters.PageNumber.toString()];
        res.redirect(url, 307);
        res.end();
        callback(url);

        parameters = null;
      } else {
        //console.log('Resize and upload CDN');

        cdnImageFlow(parameters, res, function (err, result) {
          if (err) {
            //console.log(err);
            callback(null);
          } else if (result) {
            //console.log(result);
            callback(result);
          } else {
            callback(null);
          }

          parameters = null;
        });
      }
    });
  } else {
    res.end();
    callback(null);
    parameters = null;
  }
};
