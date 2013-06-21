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

var cdnImageFlow = async.compose(enableContainerForCDN,uploadImageToCloudFile,resizeImageAndServeLocalResizeImage,downloadImages);

var cdnRedisKeyPrefix = 'CDN.';

exports.cdnImage = function cdnImage (parameters, res, callback) {

    if (parameters['PublicationID'] && parameters['Width'] && parameters['Height'] && parameters['PageNumber']) {

        var key = cdnRedisKeyPrefix+parameters['PublicationID']+'.'+parameters['Width']+'x'+parameters['Height'];

        client.HGETALL(key, function (err, obj) {

            if (err) {

                callback(null);
                parameters = null;
                key = null;
            }
            else if (obj && obj[parameters['PageNumber'].toString()]) {

                //console.log('Use CDN image '+parameters['PageNumber']);

                var url = obj['cdnURL'] + '/'+ parameters['PageNumber']+'.'+ obj[parameters['PageNumber'].toString()];
                res.redirect(url, 307)
                callback(url);

                parameters = null;
                key = null;
                url = null;
            }
            else {

                //console.log('Resize and upload CDN');

                cdnImageFlow(parameters,res,function (err, result) {

                    if (err) {
                        console.log(err);
                        callback (null);
                    }
                    else if (result) {

                        //console.log(result);
                        callback(result);
                    }
                    else callback(null);

                    parameters = null;
                    key = null;
                });
            }
        });
    }
    else {

        callback(null);
        parameters = null;
        key = null;
    }
}

var catalogRedisKeyPrefix = 'pub.';
var imageSuffix           = 'Image.ashx?ImageType=Zoom&PageNumber=';
var saveFilePathPrefix    = '../images';

function downloadImages(parameters, res, callback) {

    client.HGETALL((catalogRedisKeyPrefix+parameters.PublicationID), function (err, obj) {

        if(err) callback(err);
        else if (obj) {

            if (parseInt(parameters['PageNumber']) <= parseInt(obj['pageCount'])) {

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
            else callback('!! Page '+parameters['PageNumber']+' not exist in '+parameters['PublicationID']);
        }
        else if (!obj) callback('!! Publication '+parameters['PublicationID']+' is not exist');
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

        if (data)   callback(null, parameters, containerName);
        else        callback('!! Failed to create /'+publicationID+'/'+path.basename(resizeImageFilePath));

        publicationID = null;
        metaData = null;
        date = null;
        contentType = null;

        resizeImageFilePath = null;
        publicationInfo = null;
    });
}

function enableContainerForCDN (parameters, containerName, callback) {

    var key = cdnRedisKeyPrefix+parameters['PublicationID']+'.'+parameters['Width']+'x'+parameters['Height'];

    client.HGETALL(key, function (err, obj) {

        if (err) {

            parameters = null;
            containerName = null;
            key = null;

            callback(err);
        }
        else if (!obj) {

            //console.log('No key');

            cdnAPI.cdnEnableContainer(containerName, 3600,function(cdnEnabledContainerDetails) {

                if (cdnEnabledContainerDetails) {

                    var cdnURLInfo = {};

                    cdnURLInfo['cdnURL']    = cdnEnabledContainerDetails['x-cdn-uri'];
                    cdnURLInfo['cdnSSL']    = cdnEnabledContainerDetails['x-cdn-ssl-uri'];
                    cdnURLInfo['cdniOS']    = cdnEnabledContainerDetails['x-cdn-ios-uri'];
                    cdnURLInfo['cdnStream'] = cdnEnabledContainerDetails['x-cdn-streaming-uri'];

                    cdnURLInfo[parameters['PageNumber'].toString()] = 'jpg';

                    client.HMSET(key,cdnURLInfo,function (err, result) {
                        if (err) callback(err);
                        else     callback(null,cdnURLInfo['cdnURL']+'/'+parameters['PageNumber']+'.jpg');

                        parameters = null;
                        containerName = null;
                        key = null;

                        cdnURLInfo =null;
                        cdnEnabledContainerDetails = null;
                        result = null;
                    });
                }
                else callback('Enable container '+containerName+' failed');
            });
        }
        else if (obj) {

            //console.log('Key exist');

            obj[parameters['PageNumber'].toString()] = 'jpg';

            client.HMSET(key,obj,function (err, result) {
                if (err) callback(err);
                else     callback(null,obj['cdnURL']+'/'+parameters['PageNumber']+'.jpg');

                parameters = null;
                containerName = null;
                key = null;

                obj = null;
                result = null;
            });
        }
    });
}
