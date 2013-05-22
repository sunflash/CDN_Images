/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 15/05/13
 * Time: 15.06
 * To change this template use File | Settings | File Templates.
 */

var async = require('async');
var fs      = require('fs');
var fileSystem = require('./FileSystem');

var imageSuffix         = 'Image.ashx?ImageType=Zoom&PageNumber=';
var saveFilePathPrefix  = '../images/';

var status = 0;


exports.downloadActiveCatalogImage = function downloadActiveCatalogImage (activeCatalogs, activeCatalogsCount) {

    // ONLY one download instance at a time, status 0 no active instance, status 1 download instance running already

    if (status == 0) {

        status = 1;

        async.waterfall([

            function (callback) {

                console.log('*** Download images begin');

                // prepare data for download operation

                var activeCatalogImageFolders = [];
                var activeCatalogImagesInfo   = [];

                for (var i = 1; i <= activeCatalogsCount; i++) {

                    var catalogPubID        = activeCatalogs[i].pubID;
                    var catalogPageCount    = activeCatalogs[i].pageCount;
                    var imageLinkBody       = activeCatalogs[i].iPaperLink + imageSuffix;

                    var saveFileDirectory = saveFilePathPrefix + catalogPubID;
                    activeCatalogImageFolders.push(saveFileDirectory);

                    for (var j = 1; j <= catalogPageCount; j++ ){

                        var image = {};

                        image.saveFileDirectory  = saveFileDirectory;
                        image.saveFileName       = j + '.jpg';
                        image.imageLink          = imageLinkBody + j;

                        activeCatalogImagesInfo.push(image);
                        image = null;
                    }
                }

                console.log('downloadImageFoldersCount '+activeCatalogImageFolders.length);
                console.log('downloadImageLinksCount  '+activeCatalogImagesInfo.length);

                callback(null,activeCatalogImageFolders,activeCatalogImagesInfo);
            },
            function (activeCatalogImageFolders,activeCatalogImagesInfo,callback) {

                // Do folder check, make and keep active folders

                async.reject(activeCatalogImageFolders,

                    function(folderPath,next) {

                        fs.exists(folderPath, function(exists) {

                            if (exists) {next(true);}
                            else
                            {
                                fs.mkdir(folderPath, function(error) {if(error) console.log(error);} );
                                next(false);
                            }
                        });
                    }
                    ,function(results){

                        if (results.length > 0) console.log('Create '+results.length+' folders');
                        callback(null, activeCatalogImageFolders, activeCatalogImagesInfo);
                    });

            },
            function (activeCatalogImageFolders,activeCatalogImagesInfo,callback) {

                // Do folder check, remove unused expired folders

                fs.readdir(saveFilePathPrefix, function(err,items) {

                    if (err) {callback('Could not read content in folders ../image');}
                    else {

                        var folderExist = [];
                        for (var i = 0; i < items.length; i++ ) {
                            folderExist.push(saveFilePathPrefix+items[i]);
                        }

                        Array.prototype.diff = function(a) {
                            return this.filter(function(i) {return !(a.indexOf(i) > -1);});
                        };

                        var unusedExpiredFolders =  folderExist.diff(activeCatalogImageFolders);

                        if (unusedExpiredFolders.length > 0) {

                            async.map(unusedExpiredFolders,
                                function(unuseExpiredFolder, next){
                                    fileSystem.removeFolderRecursive(unuseExpiredFolder);
                                    next(null,unuseExpiredFolder);
                                },
                                function(err, results){

                                    if (results) console.log('Delete '+results.length+' folders');
                                });
                        }
                        activeCatalogImageFolders = null;
                        callback(null,activeCatalogImagesInfo);
                    }
                });

            },
            function (activeCatalogImagesInfo,callback) {

                // Do file check, filter out files already exist locally

                async.reject(activeCatalogImagesInfo,
                    function(imageInfo,next) {

                        var saveFilePath = imageInfo.saveFileDirectory + '/' + imageInfo.saveFileName;

                        fs.exists(saveFilePath, function(exists) {

                            if (exists) {next(true);}
                            else         next(false);
                        });
                    }
                    ,function(results){

                        activeCatalogImagesInfo = null;
                        callback(null,results);
                    });
            }
        ], function (err,downloadImagesInfo) {

            // Download images file from iPaper, max 10 simultaneous connections

            var maxDownloadSimultaneousConnections = 10;

            console.log('downloadCounts '+downloadImagesInfo.length);

            if (downloadImagesInfo.length > 0) {

                async.mapLimit(downloadImagesInfo,maxDownloadSimultaneousConnections,
                    function(imageInfo,next) {

                        var saveFilePath = imageInfo.saveFileDirectory + '/' + imageInfo.saveFileName;

                        fileSystem.downloadFileFromURL(imageInfo.imageLink,saveFilePath,function (err, success){

                            if (err) {console.log(err);}
                            //console.log(saveFilePath+'  '+imageInfo.imageLink);
                            console.log(saveFilePath);

                            if (success == true) next(null,imageInfo);
                            else                 next(null);
                        })
                    }
                    ,function(err, results){

                        if (results.length == downloadImagesInfo.length) {
                            console.log('All images download success');
                        }
                        else console.log(results.length+' of '+downloadImagesInfo.length+' succeed');

                        downloadImagesInfo = null;
                        results = null;

                        status = 0;
                    });

            }
            else {
                console.log('No image to download');
                downloadImagesInfo = null;

                status = 0;
            }

        });

    }
    else {console.log('!!! ONLY one download instance at a time');}

    console.log('***** DownloadImages ***** '+activeCatalogsCount);
}
