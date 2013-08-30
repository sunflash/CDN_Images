/**
 * Created with JetBrains WebStorm.
 * User: minwu
 * Date: 29/08/13
 * Time: 12.02
 * To change this template use File | Settings | File Templates.
 */

var async = require("async");
var path    = require('path');
var cdnClean = require('../CDN/CDN_Clean');
var fileSystem = require('../Data/FileSystem');
var catalogMetaData = require('../Data/CatalogMetaData');

var saveFilePathPrefix  = '../images';

function deleteLocalCatalogData (publicationIDs, callback) {

    async.each(publicationIDs,function(publicationID, next) {

        var catalogImageDirectory = path.join(saveFilePathPrefix,publicationID.toString());

        fileSystem.removeFolderRecursive(catalogImageDirectory);
        next();

    },function(err) {

        if (err)    {callback(err);}
        else        {callback(null,publicationIDs);}
    });
}

function cleanCatalogCDNData (publicationIDs, callback) {

    cdnClean.cleanCloudFileAndCDNCatalogData(publicationIDs, function(results){

        if (!results)                {callback(null);}
        else if (results.length > 0) {callback(null,results);}
    });
}

exports.updateCatalogData = function updateCatalogData (publicationIDs, callback) {

    var updateCatalogDataFlow = async.compose(cleanCatalogCDNData,deleteLocalCatalogData);

    updateCatalogDataFlow(publicationIDs, function (err, result) {

        if (err)         {callback(null);}
        else if (result) {callback(result);}
        else             {callback(null);}

        if (publicationIDs.length > 0) {

            catalogMetaData.getCatalogData(function (data) {
                data = null;
            });
        }
    });
};

