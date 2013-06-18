/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 18/06/13
 * Time: 10.39
 * To change this template use File | Settings | File Templates.
 */

var fs      = require('fs');
var async   = require('async');
var path    = require('path');
var redis   = require("redis"),
    client  = redis.createClient();

var cdnAPI      = require('./CDN_API');
var fileSystem  = require('../Data/FileSystem');
var resizeImage = require('../ImageIO/ResizeImage');

var cdnImageFlow = async.compose(enableContainerForCDN,uploadImageToCloudFile,resizeImageAndServeLocalResizeImage,downloadImages,cdnLinkIfEnable);

exports.cdnImage = function cdnImage (parameters, res, callback) {

    cdnImageFlow(parameters,res,function (err, result) {

        if (err) {
            console.log(err);
            callback (null);
        }
        else if (result) {

            console.log(result);
            callback(result);
        }
        else callback(null);
    });
}

function cdnLinkIfEnable (parameters, res, callback) {

    callback(null, parameters, res);
}

var catalogRedisKeyPrefix = 'pub.';
var imageSuffix           = 'Image.ashx?ImageType=Zoom&PageNumber=';
var saveFilePathPrefix    = '../images';

function downloadImages(parameters, res, callback) {

    client.HGETALL((catalogRedisKeyPrefix+parameters.PublicationID), function (err, obj) {

        if(err) callback(err);
        else {

            if (parseInt(parameters['PageNumber']) <= obj['pageCount']) {

                var savePath = path.join(saveFilePathPrefix,parameters.PublicationID,parameters.PageNumber+'.jpg');

                fs.exists(savePath, function(exists) {

                    if (exists) {

                        callback(null, parameters, res, obj);
                        savePath = null;
                    }
                    else {

                        var url = obj['iPaperLink']+imageSuffix+parameters['PageNumber'];
                        fileSystem.downloadFileFromURL(url, savePath, function (err, success) {

                            if (err)            callback(err);
                            else if (success)   callback(null, parameters, res, obj);

                            url = null;
                            savePath = null;
                        });
                    }
                });
            }
            else callback('Page '+parameters['PageNumber']+' not exist in '+parameters['PublicationID']);
        }
    });
}

function resizeImageAndServeLocalResizeImage (parameters, res, publicationInfo, callback) {

    resizeImage.resizeImage(parameters, function(resizeFilePath) {

        if (!resizeFilePath) {
            res.send(404,"Aaaa ooo!");
            res.end();
            callback('!! NO resize image '+resizeFilePath);
        }
        else {

            res.sendfile(path.resolve(resizeFilePath));
            callback(null, parameters, resizeFilePath, publicationInfo);
        }
    });
}

function uploadImageToCloudFile(parameters, resizeImageFilePath, publicationInfo, callback) {

    // Set, update : X-Object-Meta-Book: 'Hello world'
    // Delete      : X-Remove-Object-Meta-Name: foo

    var publicationID = parameters['PublicationID'];

    var containerName = publicationID+'_'+parameters['Width']+'x'+parameters['Height'];

    var metaData = {'X-Object-Meta-ID': publicationID.toString()};
    var date     = new Date(publicationInfo['pubStop']);

    var contentType     = 'image/jpeg';

    cdnAPI.createUpdateObject(resizeImageFilePath, containerName, contentType, metaData, date, function (data) {

        if (data)   callback(null, parameters);
        else        callback('!! Failed to create /'+publicationID+'/'+path.basename(resizeImageFilePath));

        publicationID = null;
        metaData = null;
        date = null;
        contentType = null;

        resizeImageFilePath = null;
        publicationInfo = null;
    });
}

var cdnRedisKeyPrefix = 'CDN.';

function enableContainerForCDN (parameters, callback) {

    var key = cdnRedisKeyPrefix+parameters['PublicationID']+'.'+parameters['Width']+'x'+parameters['Height'];

    console.log(key);

    //client.HGETALL(())

}
