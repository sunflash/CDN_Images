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
var cdnAPI  = require('./CDN_API');

exports.cdnImage = function cdnImage (parameters, res, callback) {

    res.json(parameters);

}

/*
 resizeImage.resizeImage(query, function (data) {

 if (!data) {
 res.send(404,"Aaaa ooo!");
 res.end();
 }
 else res.sendfile(path.resolve(data));

 });*/