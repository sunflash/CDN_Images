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

var saveFilePathPrefix  = '../images/';

exports.resizeImage = function resizeImage (resizeParameters,callback) {

    var  resizeImageFlow  = async.compose(resizeRequestImage,createResizeImageFolderIfNotExist,checkImageExistLocal);

    resizeImageFlow(resizeParameters,function (err, result) {

        if (err) {
            console.log(err);
            callback (null);
        }
        else if (result)   callback(result);
        else               callback(null);
    });

    console.log('***** ResizeImage *****');
}

function checkImageExistLocal (parameters, callback) {

    var imageFolderPath = saveFilePathPrefix + parameters.PublicationID;
    var imageFilePath   = imageFolderPath  + '/' + parameters.PageNumber + ".jpg";

    fs.exists(imageFilePath, function(exists) {

        if (!exists) callback('Image NOT exist');
        else         callback(null,parameters,imageFolderPath);
    });
}

function createResizeImageFolderIfNotExist (parameters,imageFolderPath,callback) {

    var saveFileFolderPath = imageFolderPath+'/'+parameters.Width+'x'+parameters.Height;

    fs.exists(saveFileFolderPath, function(exists) {

        if (!exists) {

            fs.mkdir(saveFileFolderPath, function(error) {

                if(error) callback(error);
                else      callback(null,parameters,imageFolderPath,saveFileFolderPath);

            });
        }
        else callback(null,parameters,imageFolderPath,saveFileFolderPath);
    });
}


function resizeRequestImage (parameters,imageFolderPath,saveFileFolderPath,callback) {

    var filename      = parameters.PageNumber+'.jpg';
    var imageFilePath = imageFolderPath + '/' + filename;
    var saveFilePath  = saveFileFolderPath + '/' + filename;
    var readStream = fs.createReadStream(imageFilePath);
    var writeStream = fs.createWriteStream(saveFilePath);

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

