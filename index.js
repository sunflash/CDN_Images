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

var cluster = require('cluster');
var numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
    // Fork workers. One per CPU for maximum effectiveness
    for (var i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    cluster.on('exit', function(deadWorker, code, signal) {
        // Restart the worker
        var worker = cluster.fork();

        // Note the process IDs
        var newPID = worker.process.pid;
        var oldPID = deadWorker.process.pid;

        // Log the event
        console.log('worker '+oldPID+' died.');
        console.log('worker '+newPID+' born.');
    });
} else {

    app.listen(80);
}

//------------------------------------------------- '/' '/mr'

app.get('/', function(req, res) {

    res.send("MR Debian Test Server");
    res.end();
});

app.get('/mr', function(req, res){

    res.redirect('http://www.minreklame.dk', 307);
});

//------------------------------------------------ '/image'

var path        =  require('path');
var cdnImage    =  require('./CDN/CDN_Image');

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

        cdnImage.cdnImage(query, res);

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

//------------------------------------------------------ '/debug'

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

//------------------------------------------------------ '/api'

var cdnAPI = require('./CDN/CDN_API');

app.get('/api', function(req, res) {

    if(req.query.mode) {

        if (req.query.mode == 'authDetails') {

            cdnAPI.authDetails(function (data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'accountDetails') {

            cdnAPI.accountDetails(function (data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'containerDetails') {

            cdnAPI.containerDetails(req.query.containerName,function(data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'containerList') {

            cdnAPI.containerList(function(data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'createContainer') {

            // Set metadata : X-Container-Meta-Book: 'Hello world'

            var metaData;
            //metaData = {'X-Container-Meta-Ghost': 'Buster', 'X-Container-Meta-super': 'man'};

            cdnAPI.createContainer(req.query.containerName, metaData, function(data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'setUpdateDeleteContainerMetaData') {

            // Set, update : X-Container-Meta-Book: 'Hello world'
            // Delete      : X-Remove-Container-Meta-Name: foo

            var metaData;
            metaData = {'X-Remove-Container-Meta-Ghost': 'Buster', 'X-Container-Meta-Iron': 'man'};

                cdnAPI.setUpdateDeleteContainerMetaData(req.query.containerName,metaData,function(data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'getContainerObjects') {

            cdnAPI.getContainerObjects(req.query.containerName, function(data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'deleteSingleObject') {

            cdnAPI.deleteSingleObject(req.query.containerName, req.query.objectName, function (data) {
               outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'deleteMultipleObjects') {

            cdnAPI.deleteMultipleObjects(req.query.containerName, req.query.objectNames.split(',') , function (data) {
               outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'deleteAllObjectsInContainer') {

            cdnAPI.deleteAllObjectsInContainer(req.query.containerName, function (data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'deleteContainers') {

            var containerNames = req.query.containerNames;
            if (containerNames.indexOf(',') != -1) {containerNames = containerNames.split(',');}

            cdnAPI.deleteContainers(containerNames, function (data) {
                outputDataJSON(data,res);
                containerNames = null;
            });
        }
        else if (req.query.mode == 'createUpdateObject') {

            // X-Object-Meta-PIN: 1234

            var containerName   = 'Test';

            //var date        = new Date("June 12, 2013 16:30:00"); // Your timezone!
            var date          = new Date().addHours(8);

            var filePath        = '../images/22959/1.jpg';
            var contentType     = 'image/jpeg';
            var metaData        =  {'X-Object-Meta-Ikea': 'kitchen catalog'};

            /*
            var filePath        = '../publication.json';
            var contentType     = 'application/json';
            var metaData        =  {'X-Object-Meta-Feed': 'publication'};
            */

            cdnAPI.createUpdateObject(filePath, containerName, contentType, metaData, date, function (data) {
                outputDataJSON(data,res);

                containerName = null;
                date = null;
                filePath = null;
                contentType = null;
                metaData = null;
            });
        }
        else if (req.query.mode == 'getObjectMetaData') {

            cdnAPI.objectDetails(req.query.containerName, req.query.objectName, function(data) {
                outputDataJSON(data,res);
            });
        }
        else if (req.query.mode == 'updateObjectMetaData') {

            // Set, update : X-Object-Meta-Book: 'Hello world'
            // Delete      : X-Remove-Object-Meta-Name: foo

            var metaData;
            metaData = {'X-Remove-Object-Meta-Ikea': 'kitchen catalog', 'X-Object-Meta-catalog': 'ikea'};

            //var date = new Date("June 12, 2013 16:1:00"); // Your timezone!
            var date = new Date().addHours(5);

            cdnAPI.updateObjectMetaData(req.query.containerName, req.query.objectName, metaData, date, function (statusCode) {
                outputDataJSON(statusCode,res);

                metaData = null;
                date = null;
            });
        }
        else if (req.query.mode == 'copyObject') {

            if (req.query.parameters.indexOf(',') != -1) {

                var p = req.query.parameters.split(',');
                var metaData = {'X-Object-Meta-copy': 'object'};

                cdnAPI.copyObject(p[0],p[1],p[2],p[3],metaData, function(statusCode) {
                    outputDataJSON(statusCode,res);

                    p = null;
                    metaData = null;
                })
            }
            else outputDataJSON(null, res);
        }
        else if (req.query.mode == 'moveObject') {

            if (req.query.parameters.indexOf(',') != -1) {

                var p = req.query.parameters.split(',');
                var metaData = {'X-Object-Meta-move': 'object'};

                cdnAPI.moveObject(p[0],p[1],p[2],p[3],metaData, function(statusCode) {
                    outputDataJSON(statusCode,res);

                    p = null;
                    metaData = null;
                })
            }
            else outputDataJSON(null, res);
        }
        else if (req.query.mode == 'renameObject') {

            if (req.query.parameters.indexOf(',') != -1) {

                var p = req.query.parameters.split(',');
                var metaData = {'X-Object-Meta-rename': 'object'};

                cdnAPI.renameUpdateObject(p[0],p[1],p[2],metaData, function(statusCode) {
                    outputDataJSON(statusCode,res);

                    p = null;
                    metaData = null;
                })
            }
            else outputDataJSON(null, res);
        }
        else if (req.query.mode == 'downloadObject') {

            if (req.query.parameters.indexOf(',') != -1) {

                var p = req.query.parameters.split(',');

                cdnAPI.downloadObject(p[0],p[1],p[2], function(statusCode) {
                    outputDataJSON(statusCode,res);

                    p = null;
                })
            }
            else outputDataJSON(null, res);
        }
        else if (req.query.mode == 'cdnEnabledContainerList') {

            cdnAPI.cdnEnabledContainerList(function(cdnEnabledContainerList){

                outputDataJSON(cdnEnabledContainerList,res);
            });
        }
        else if (req.query.mode == 'cdnEnabledContainerDetails') {

            cdnAPI.cdnEnabledContainerDetails(req.query.containerName,function(cdnEnabledContainerDetails) {

                outputDataJSON(cdnEnabledContainerDetails,res);
            });
        }
        else if (req.query.mode == 'cdnEnableContainer') {

            cdnAPI.cdnEnableContainer(req.query.containerName, req.query.ttl, function (cdnEnabledContainerDetails) {

                outputDataJSON(cdnEnabledContainerDetails, res);
            });
        }
        else if (req.query.mode == 'cdnDisableContainer') {

            cdnAPI.cdnDisableContainer(req.query.containerName, function(statusCode) {
                outputDataJSON(statusCode, res);
            })
        }
        else if (req.query.mode == 'changeCDNContainerAttributes') {

            var containerName   = null;
            var TTL             = null;
            var cdnEnable       = null;
            var logRetention    = null;

            if (req.query.containerName) containerName  = req.query.containerName;
            if (req.query.TTL)           TTL            = req.query.TTL;
            if (req.query.cdnEnable)     cdnEnable      = req.query.cdnEnable;
            if (req.query.logRetention)  logRetention   = req.query.logRetention;

            cdnAPI.changeCDNContainerAttributes(containerName, TTL, cdnEnable, logRetention , function (containerDetails) {

                outputDataJSON(containerDetails, res);
            });
        }
        else res.end();
    }
    else res.end();
});

Date.prototype.addHours= function(h){
    this.setHours(this.getHours()+h);
    return this;
}

function outputDataJSON (data,res) {

    if (data)   res.json(data);
    else        res.send(404,"Aaaa ooo!");

    res.end();
}

//------------------------------------------------------
