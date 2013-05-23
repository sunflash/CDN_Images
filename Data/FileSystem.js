/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 15/05/13
 * Time: 14.58
 * To change this template use File | Settings | File Templates.
 */

var fs = require('fs');
var exec = require('child_process').exec,child;
var request = require('request');

exports.removeFolderRecursive = function removeFolderRecursive (folderPath){

    var deleteFolderCommand = 'rm -rf '+folderPath;
    exec(deleteFolderCommand,function(err,out) {
        console.log(out); err && console.log(err);
    });
}

exports.downloadFileFromURL = function saveFileFromURL (url, savePath, callback) {

    var localStream = fs.createWriteStream(savePath);

    var out = request({ uri: url });
    out.setMaxListeners(0);

    out.on('response', function (resp) {

        if (resp.statusCode === 200){

            out.pipe(localStream);

            localStream.on('close', function () {
                callback(null, 1);
            });

            localStream.on('error', function (){

                fs.unlink(savePath, function (err) {
                    if (err) callback(err);
                    else {
                        callback(new Error('successfully deleted '+savePath),null);
                    }
                });
            });
        }
        else {

            fs.unlink(savePath, function (err) {
                if (err) callback(err);
                else {
                    console.log('successfully deleted '+savePath);
                    callback(new Error("No file found at url : "+url),null);
                }
            });
        }
    });
}
