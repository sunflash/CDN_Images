/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 06/05/13
 * Time: 17.05
 * To change this template use File | Settings | File Templates.
 */

//-------------------------------------------------------------------------------

// Start CronJob

var nodeCron = require('./nodeCron');
nodeCron.cron();

//-------------------------------------------------------------------------------

// Express router

var express = require('express');

var app = express ();
app.use(express.compress());
app.disable('x-powered-by');

//-------------------------------------------------

app.get('/', function(req, res) {

    res.send("MR Debian Test Server");
    res.end();
});

app.get('/mr', function(req, res){

    res.redirect('http://www.minreklame.dk', 307);
});

//------------------------------------------------

var resizeImage =  require('./ImageIO/ResizeImage');
var path        =  require('path');

app.get('/image', function(req, res){

    var publicationID = req.query.id;
    var page = req.query.p;
    var width = req.query.w;
    var height =  req.query.h;

    if (publicationID && page && width && height) {

        var query = {
            "PublicationID":publicationID,
            "PageNumber":page,
            "Width":width,
            "Height":height
        }

        resizeImage.resizeImage(query, function (data) {

            if (!data) {
                res.send(404,"Aaaa ooo!");
                res.end();
            }
            else res.sendfile(path.resolve(data));

        });

        publicationID = null;
        page = null;
        width = null;
        height = null;
    }
    else {
        res.send(404,"Aaaa ooo!");
        res.end();
    }
});

//------------------------------------------------------

var catalogMetaData = require('./Data/CatalogMetaData');

app.get('/debug', function(req, res){

    catalogMetaData.getCatalogData(
        function (data) {

            if (!data) {
                res.send(404,"Aaaa ooo!");
                res.end();
            }
            else {
                res.json(data);
                res.end();
                console.log('JSON output');
            }
        }
    );
});

//------------------------------------------------------

var cdnAPI = require('./CDN/CDN_API');

app.get('/api', function(req, res) {

    cdnAPI.authenticate(function(data) {

        if (data) {
            res.json(data);
            res.end();
        }
        else {
            res.send(404,"Aaaa ooo!");
            res.end();
        }
    })
});


app.listen(80);