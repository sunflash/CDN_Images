/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 22/05/13
 * Time: 15.39
 * To change this template use File | Settings | File Templates.
 */

var fs      = require('fs');
var async   = require('async');
var gm      = require('gm');
var path    = require('path');

var saveFilePathPrefix  = '../images';

exports.resizeImage = function resizeImage (resizeParameters,callback) {

    var resizeImagePath = path.join(saveFilePathPrefix,resizeParameters.PublicationID.toString(),(resizeParameters.Width+'x'+resizeParameters.Height),(resizeParameters.PageNumber+'.jpg'));

    fs.exists(resizeImagePath, function(exists) {

        if (exists) {
            console.log('ResizeImageExist '+resizeImagePath);
            callback(resizeImagePath);
        }
        else {

            var  resizeImageFlow  = async.compose(resizeRequestImage,createResizeImageFolderIfNotExist,checkImageExistLocal);

            resizeImageFlow(resizeParameters,function (err, result) {

                if (err) {
                    console.log(err);
                    callback (null);
                }
                else if (result)   callback(result);
                else               callback(null);
            });
        }
    });

    console.log('***** ResizeImage *****');
}

function checkImageExistLocal (parameters, callback) {

    var imageFilePath = path.join(saveFilePathPrefix, parameters.PublicationID.toString(), (parameters.PageNumber+'.jpg'));

    fs.exists(imageFilePath, function(exists) {

        if (!exists) callback('Image NOT exist');
        else         callback(null,parameters,path.dirname(imageFilePath));
    });
}

function createResizeImageFolderIfNotExist (parameters,imageFolderPath,callback) {

    var saveFileFolderPath = path.join(imageFolderPath,(parameters.Width+'x'+parameters.Height));

    fs.exists(saveFileFolderPath, function(exists) {

        if (!exists) {

            fs.mkdir(saveFileFolderPath, function(error) {

                callback(null,parameters,imageFolderPath,saveFileFolderPath);

            });
        }
        else callback(null,parameters,imageFolderPath,saveFileFolderPath);
    });
}


function resizeRequestImage (parameters,imageFolderPath,saveFileFolderPath,callback) {

    var filename      = parameters.PageNumber+'.jpg';
    var imageFilePath = path.join(imageFolderPath,filename);
    var saveFilePath  = path.join(saveFileFolderPath,filename);
    var readStream = fs.createReadStream(imageFilePath);
    var writeStream = fs.createWriteStream(saveFilePath);

    readStream.setMaxListeners(0);
    writeStream.setMaxListeners(0);

    gm(readStream,filename)
        .resize(parameters.Width,parameters.Height)
        .stream()
        .pipe(writeStream);

    writeStream.on('close', function () {
        callback(null,saveFilePath);
    });

    writeStream.on('error', function (){

        fs.unlink(saveFilePath, function (err) {
            if (err) callback(err);
            else {
                callback(new Error('successfully deleted '+saveFilePath),null);
            }
        });
    });

}

