/**
 * Created with JetBrains PhpStorm.
 * User: minwu
 * Date: 27/05/13
 * Time: 13.49
 * To change this template use File | Settings | File Templates.
 */

var request = require("request");
var redis = require("redis"),
  client = redis.createClient();
var async = require("async");
var crypto = require("crypto");
var path = require("path");
var fs = require("fs");

var user = "";
var id = "";
var authLink = "https://lon.identity.api.rackspacecloud.com/v1.0";

//--------------------------------------------------------------------------------

// URL encode container name, cut to under 256 byte string and replace '/' with '_'

function encodeContainerName(containerName, callback) {
  if (containerName) {
    if (containerName.length > 0) {
      containerName = containerName.replace(/\//g, "_");
      containerName = encodeURIComponent(containerName);

      if (containerName.length > 250) {
        containerName = containerName.substr(0, 250);
      }

      callback(containerName);
    } else {
      callback(null);
    }
  } else {
    callback(null);
  }
}

function encodeContainerNames(containerNames, callback) {
  var encodedContainerNames = [];

  for (var i = 0; i < containerNames.length; i++) {
    var containerName = containerNames[i];

    if (containerName) {
      if (containerName.length > 0) {
        containerName = containerName.replace(/\//g, "_");
        containerName = encodeURIComponent(containerName);

        if (containerName.length > 250) {
          containerName = containerName.substr(0, 1000);
        }

        encodedContainerNames.push(containerName);
      }
    }
  }

  if (encodedContainerNames.length > 0) {
    callback(encodedContainerNames);
  } else {
    callback(null);
  }
}

// URL encode object name, cut to under 1024 byte string

function encodeObjectName(objectName, callback) {
  if (objectName) {
    if (objectName.length > 0) {
      objectName = objectName.replace(/\//g, "_");
      objectName = encodeURIComponent(objectName);

      if (objectName.length > 1000) {
        objectName = objectName.substr(0, 1000);
      }

      callback(objectName);
    } else {
      callback(null);
    }
  } else {
    callback(null);
  }
}

function encodeObjectNames(objectNames, callback) {
  var encodedObjectNames = [];

  for (var i = 0; i < objectNames.length; i++) {
    var objectName = objectNames[i];

    if (objectName) {
      if (objectName.length > 0) {
        objectName = objectName.replace(/\//g, "_");
        objectName = encodeURIComponent(objectName);

        if (objectName.length > 1000) {
          objectName = objectName.substr(0, 1000);
        }

        encodedObjectNames.push(objectName);
      }
    }
  }

  if (encodedObjectNames.length > 0) {
    callback(encodedObjectNames);
  } else {
    callback(null);
  }
}

function encodeMarkerName(markerName) {
  if (markerName) {
    if (markerName.length > 0) {
      markerName = markerName.replace(/\//g, "_");
      markerName = encodeURIComponent(markerName);

      if (markerName.length > 250) {
        markerName = markerName.substr(0, 250);
      }

      return markerName;
    } else {
      return null;
    }
  } else {
    return null;
  }
}

//--------------------------------------------------------------------------------

// Authentication to rackspace CDN

var authInfo = {};
var authRedis = "cdn.auth";

function authenticate(callback) {
  request(
    {
      method: "GET",
      uri: authLink,
      headers: {
        "X-Auth-User": user,
        "X-Auth-Key": id,
      },
    },
    function (error, response, body) {
      //console.log(response.statusCode);
      //console.log(response.headers);

      if (body) {
        //console.log(body);
      }

      if (response) {
        if (response.statusCode === 204 || response.statusCode === 202) {
          authInfo.authToken = response.headers["x-auth-token"];
          authInfo.serverURL = response.headers["x-server-management-url"];
          authInfo.storageURL = response.headers["x-storage-url"];
          authInfo.cdnURL = response.headers["x-cdn-management-url"];
          authInfo.storageToken = response.headers["x-storage-token"];

          var now = new Date();
          now.setSeconds(
            now.getSeconds() +
              parseInt(response.headers["cache-control"].split("=")[1], 10)
          );
          authInfo.expireDate = now;

          //console.log(authInfo);
          //console.log('ExpireInSec '+response.headers['cache-control'].split("=")[1]);

          client.HMSET(authRedis, authInfo, function (err, obj) {
            if (err) {
              callback(err);
            }
            //else     console.dir(obj);

            if (obj) {
            }

            callback(authInfo);
          });
        } else {
          callback(null);
        }
      } else {
        callback(null);
      }
    }
  );
}

function getAuthInfo(callback) {
  if (!authInfo.authToken) {
    client.HGETALL(authRedis, function (err, authInfoRedis) {
      if (err) {
        callback(null);
      } else if (authInfoRedis && authInfoRedis.authToken) {
        //console.log('Loading authInfo from local redis db');
        authInfo = authInfoRedis;
        callback(authInfoRedis);
      } else {
        authenticate(function (authInfoFresh) {
          //console.log('Fresh authInfo from rackspace');
          callback(authInfoFresh);
        });
      }
    });
  } else {
    //console.log('Reuse authInfo from global variable');
    callback(authInfo);
  }
}

//--------------------------------------------------------------------------------

// Get Cloud Files account details

function getAccountDetails(callback) {
  getAuthInfo(function (api) {
    request(
      {
        method: "HEAD",
        uri: api.storageURL,
        headers: {
          "X-Auth-Token": api.authToken,
        },
      },
      function (error, response, body) {
        if (body) {
        }

        if (response.statusCode === 204) {
          var accountDetails = {};
          accountDetails.containerCount =
            response.headers["x-account-container-count"];
          accountDetails.objectCount =
            response.headers["x-account-object-count"];
          accountDetails.bytesUsed = response.headers["x-account-bytes-used"];

          callback(accountDetails);
        } else if (response.statusCode === 401) {
          authenticate(function (authInfoFresh) {
            request(
              {
                method: "HEAD",
                uri: api.storageURL,
                headers: {
                  "X-Auth-Token": authInfoFresh.authToken,
                },
              },
              function (error, response, body) {
                if (body) {
                }

                if (response.statusCode === 204) {
                  var accountDetails = {};
                  accountDetails.containerCount =
                    response.headers["x-account-container-count"];
                  accountDetails.objectCount =
                    response.headers["x-account-object-count"];
                  accountDetails.bytesUsed =
                    response.headers["x-account-bytes-used"];

                  callback(accountDetails);
                } else {
                  callback(null);
                }
              }
            );
          });
        } else {
          callback(null);
        }
      }
    );
  });
}

//--------------------------------------------------------------------------------

// Get list of container in json, max 10000 containers for each fetch

function getContainerListInChunk(
  maxContainersFetchLimit,
  maxRound,
  round,
  marker,
  containers,
  callback
) {
  if (!maxContainersFetchLimit || maxContainersFetchLimit > 10000) {
    maxContainersFetchLimit = 10000;
  }
  if (!containers) {
    containers = [];
  }
  if (!round) {
    round = 0;
  }

  if (round < maxRound) {
    getAuthInfo(function (api) {
      var url =
        api.storageURL + "?format=json" + "&limit=" + maxContainersFetchLimit;
      if (marker) {
        url = url + "&marker=" + encodeMarkerName(marker);
      }

      request(
        {
          method: "GET",
          uri: url,
          headers: {
            "X-Auth-Token": api.authToken,
          },
        },
        function (error, response, body) {
          if (response.statusCode === 200) {
            var containerArray = JSON.parse(body);

            if (containerArray.length > 0) {
              containers = containers.concat(containerArray);

              round++;
              if (round < maxRound) {
                marker = null;
                var lastContainerName =
                  containerArray[containerArray.length - 1].name;
                getContainerListInChunk(
                  maxContainersFetchLimit,
                  maxRound,
                  round,
                  lastContainerName,
                  containers,
                  callback
                );
              } else if (round === maxRound) {
                callback(containers);
                marker = null;
              }
            } else {
              callback(null);
            }
          } else if (response.statusCode === 204) {
            callback(null);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              request(
                {
                  method: "GET",
                  uri: url,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                  },
                },
                function (error, response, body) {
                  if (response.statusCode === 200) {
                    var containerArray = JSON.parse(body);

                    if (containerArray.length > 0) {
                      containers = containers.concat(containerArray);

                      round++;
                      if (round < maxRound) {
                        marker = null;
                        var lastContainerName =
                          containerArray[containerArray.length - 1].name;
                        getContainerListInChunk(
                          maxContainersFetchLimit,
                          maxRound,
                          round,
                          lastContainerName,
                          containers,
                          callback
                        );
                      } else if (round === maxRound) {
                        callback(containers);
                        marker = null;
                      }
                    } else {
                      callback(null);
                    }
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  } else {
    callback(null);
  }
}

function getContainerList(maxContainersFetchLimit, callback) {
  if (!maxContainersFetchLimit || maxContainersFetchLimit > 10000) {
    maxContainersFetchLimit = 10000;
  }

  getAccountDetails(function (accountDetails) {
    if (accountDetails) {
      var containersCount = parseInt(accountDetails.containerCount, 10);
      var maxRound = Math.ceil(containersCount / maxContainersFetchLimit);

      getContainerListInChunk(
        maxContainersFetchLimit,
        maxRound,
        null,
        null,
        null,
        function (containerObjects) {
          callback(containerObjects);
        }
      );
    } else {
      callback(null);
    }
  });
}

//--------------------------------------------------------------------------------

// Get Container details

function getContainerDetails(containerName, callback) {
  getAuthInfo(function (api) {
    request(
      {
        method: "HEAD",
        uri: api.storageURL + "/" + containerName + "?format=json",
        headers: {
          "X-Auth-Token": api.authToken,
        },
      },
      function (error, response, body) {
        if (body) {
        }

        if (response.statusCode === 204) {
          var containerDetails = {};
          containerDetails.objectsCount =
            response.headers["x-container-object-count"];
          containerDetails.bytesUsed =
            response.headers["x-container-bytes-used"];

          var metaTag;
          var metaHeaderPrefix = "x-container-meta-";
          var log = "x-container-meta-access-log-delivery";

          for (var x in response.headers) {
            if (x.indexOf(metaHeaderPrefix) !== -1 && x !== log) {
              if (!metaTag) {
                metaTag = {};
              }
              metaTag[
                x.substr(
                  x.indexOf(metaHeaderPrefix) + metaHeaderPrefix.length,
                  x.length - metaHeaderPrefix.length
                )
              ] = response.headers[x];
            }
          }

          if (metaTag) {
            containerDetails.metaTag = metaTag;
          }

          callback(containerDetails);
        } else if (response.statusCode === 401) {
          authenticate(function (authInfoFresh) {
            request(
              {
                method: "HEAD",
                uri: api.storageURL + "/" + containerName + "?format=json",
                headers: {
                  "X-Auth-Token": authInfoFresh.authToken,
                },
              },
              function (error, response, body) {
                if (body) {
                }

                if (response.statusCode === 204) {
                  var containerDetails = {};
                  containerDetails.objectsCount =
                    response.headers["x-container-object-count"];
                  containerDetails.bytesUsed =
                    response.headers["x-container-bytes-used"];

                  var metaTag;
                  var metaHeaderPrefix = "x-container-meta-";
                  var log = "x-container-meta-access-log-delivery";

                  for (var x in response.headers) {
                    if (x.indexOf(metaHeaderPrefix) !== -1 && x !== log) {
                      if (!metaTag) {
                        metaTag = {};
                      }
                      metaTag[
                        x.substr(
                          x.indexOf(metaHeaderPrefix) + metaHeaderPrefix.length,
                          x.length - metaHeaderPrefix.length
                        )
                      ] = response.headers[x];
                    }
                  }

                  if (metaTag) {
                    containerDetails.metaTag = metaTag;
                  }

                  callback(containerDetails);
                } else {
                  callback(null);
                }
              }
            );
          });
        } else {
          callback(null);
        }
      }
    );
  });
}

//--------------------------------------------------------------------------------

// Create new container

/* NOTICE :
 you can have 4096 bytes maximum overall metadata, with 90 distinct metadata items at the most.
 Each may have a 128 character name length with a 256 max value length each.
 Any valid UTF-8 http header value is allowed for metadata.
 */

// Set metadata : X-Container-Meta-Book: 'Hello world'

function createCloudFileContainer(containerName, metaData, callback) {
  getAuthInfo(function (api) {
    var headerValues = {};
    if (metaData) {
      headerValues = metaData;
    }
    headerValues["X-Auth-Token"] = api.authToken;

    request(
      {
        method: "PUT",
        uri: api.storageURL + "/" + containerName,
        headers: headerValues,
      },
      function (error, response, body) {
        if (body) {
        }

        if (response && response.statusCode === 201) {
          callback(1);
        } else if (response && response.statusCode === 202) {
          callback(2);
        } else if (response && response.statusCode === 401) {
          authenticate(function (authInfoFresh) {
            headerValues["X-Auth-Token"] = authInfoFresh.authToken;

            request(
              {
                method: "PUT",
                uri: api.storageURL + "/" + containerName,
                headers: headerValues,
              },
              function (error, response, body) {
                if (body) {
                }

                if (response.statusCode === 201) {
                  callback(1);
                } else if (response.statusCode === 202) {
                  callback(2);
                } else {
                  callback(null);
                }
              }
            );
          });
        } else {
          callback(null);
        }
      }
    );
  });
}

//--------------------------------------------------------------------------------

// Set update and delete container meta data
// Set, update : X-Container-Meta-Book: 'Hello world'
// Delete      : X-Remove-Container-Meta-Name: foo

function setUpdateDeleteCloudFileContainerMetaData(
  containerName,
  metaData,
  callback
) {
  getAuthInfo(function (api) {
    var headerValues = {};
    if (metaData) {
      headerValues = metaData;
    }
    headerValues["X-Auth-Token"] = api.authToken;

    request(
      {
        method: "POST",
        uri: api.storageURL + "/" + containerName,
        headers: headerValues,
      },
      function (error, response, body) {
        if (body) {
        }

        if (response.statusCode === 204) {
          callback(1);
        } else if (response.statusCode === 404) {
          callback(null);
        } else if (response.statusCode === 401) {
          authenticate(function (authInfoFresh) {
            headerValues["X-Auth-Token"] = authInfoFresh.authToken;

            request(
              {
                method: "POST",
                uri: api.storageURL + "/" + containerName,
                headers: headerValues,
              },
              function (error, response, body) {
                if (body) {
                }

                if (response.statusCode === 204) {
                  callback(1);
                } else if (response.statusCode === 404) {
                  callback(null);
                } else {
                  callback(null);
                }
              }
            );
          });
        } else {
          callback(null);
        }
      }
    );
  });
}

//--------------------------------------------------------------------------------

// List objects in container, max 10000 objects for each fetch

function getCloudFileContainerObjectsInChunk(
  containerName,
  maxObjectsFetchLimit,
  maxRound,
  round,
  marker,
  objects,
  callback
) {
  if (!maxObjectsFetchLimit || maxObjectsFetchLimit > 10000) {
    maxObjectsFetchLimit = 10000;
  }
  if (!objects) {
    objects = [];
  }
  if (!round) {
    round = 0;
  }

  if (round < maxRound) {
    getAuthInfo(function (api) {
      var url =
        api.storageURL +
        "/" +
        containerName +
        "?format=json" +
        "&limit=" +
        maxObjectsFetchLimit;
      if (marker) {
        url = url + "&marker=" + encodeMarkerName(marker);
      }

      request(
        {
          method: "GET",
          uri: url,
          headers: {
            "X-Auth-Token": api.authToken,
          },
        },
        function (error, response, body) {
          if (response.statusCode === 200) {
            var objectsArray = JSON.parse(body);

            if (objectsArray.length > 0) {
              objects = objects.concat(objectsArray);

              round++;
              if (round < maxRound) {
                marker = null;
                var lastObjectName = objectsArray[objectsArray.length - 1].name;
                getCloudFileContainerObjectsInChunk(
                  containerName,
                  maxObjectsFetchLimit,
                  maxRound,
                  round,
                  lastObjectName,
                  objects,
                  callback
                );
              } else if (round === maxRound) {
                callback(objects);
                marker = null;
              }
            } else {
              callback(null);
            }
          } else if (response.statusCode === 204) {
            //console.log('NO objects in container');
            callback(null);
          } else if (response.statusCode === 404) {
            //console.log('container not exist');
            callback(null);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              request(
                {
                  method: "GET",
                  uri: url,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                  },
                },
                function (error, response, body) {
                  if (response.statusCode === 200) {
                    var objectsArray = JSON.parse(body);

                    if (objectsArray.length > 0) {
                      objects = objects.concat(objectsArray);

                      round++;
                      if (round < maxRound) {
                        marker = null;
                        var lastObjectName =
                          objectsArray[objectsArray.length - 1].name;
                        getCloudFileContainerObjectsInChunk(
                          containerName,
                          maxObjectsFetchLimit,
                          maxRound,
                          round,
                          lastObjectName,
                          objects,
                          callback
                        );
                      } else if (round === maxRound) {
                        callback(objects);
                        marker = null;
                      }
                    } else {
                      callback(null);
                    }
                  } else if (response.statusCode === 204) {
                    //console.log('NO objects in container');
                    callback(null);
                  } else if (response.statusCode === 404) {
                    //console.log('container not exist');
                    callback(null);
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  } else {
    callback(null);
  }
}

function getCloudFileContainerObjects(
  containerName,
  maxObjectsFetchLimit,
  callback
) {
  if (!maxObjectsFetchLimit || maxObjectsFetchLimit > 10000) {
    maxObjectsFetchLimit = 10000;
  }

  getContainerDetails(containerName, function (containerDetails) {
    if (containerDetails) {
      var objectsCount = parseInt(containerDetails.objectsCount, 10);
      var maxRound = Math.ceil(objectsCount / maxObjectsFetchLimit);

      getCloudFileContainerObjectsInChunk(
        containerName,
        maxObjectsFetchLimit,
        maxRound,
        null,
        null,
        null,
        function (containerObjects) {
          callback(containerObjects);
        }
      );
    } else {
      callback(null);
    }
  });
}

//--------------------------------------------------------------------------------

// Delete single object, one at a time

function deleteSingleObjectInCloudFileContainer(
  containerName,
  objectName,
  callback
) {
  getAuthInfo(function (api) {
    request(
      {
        method: "DELETE",
        uri: api.storageURL + "/" + containerName + "/" + objectName,
        headers: {
          "X-Auth-Token": api.authToken,
        },
      },
      function (error, response, body) {
        if (body) {
        }

        if (response.statusCode === 204) {
          callback(1);
        } else if (response.statusCode === 404) {
          //console.log('Object not exist A');
          callback(null);
        } else if (response.statusCode === 401) {
          authenticate(function (authInfoFresh) {
            request(
              {
                method: "DELETE",
                uri: api.storageURL + "/" + containerName + "/" + objectName,
                headers: {
                  "X-Auth-Token": authInfoFresh.authToken,
                },
              },
              function (error, response, body) {
                if (body) {
                }

                if (response.statusCode === 204) {
                  callback(1);
                } else if (response.statusCode === 404) {
                  //console.log('Object not exist B');
                  callback(null);
                } else {
                  callback(null);
                }
              }
            );
          });
        } else {
          callback(null);
        }
      }
    );
  });
}

//--------------------------------------------------------------------------------

// Delete multiple objects, max 10000 objects for each delete operation

function deleteCloudFileContainerObjectsInChunk(
  containerName,
  objectsNames,
  maxObjectsDeleteLimit,
  maxRound,
  round,
  callback
) {
  if (!maxObjectsDeleteLimit || maxObjectsDeleteLimit > 10000) {
    maxObjectsDeleteLimit = 10000;
  }
  if (objectsNames.length < maxObjectsDeleteLimit) {
    maxObjectsDeleteLimit = objectsNames.length;
  }
  if (!round) {
    round = 0;
  }

  var deleteObjectList = "";

  for (var i = 0; i < maxObjectsDeleteLimit; i++) {
    if (i < maxObjectsDeleteLimit - 1) {
      deleteObjectList += "/" + containerName + "/" + objectsNames.pop() + "\n";
    } else {
      deleteObjectList += "/" + containerName + "/" + objectsNames.pop();
    }
  }

  if (round < maxRound) {
    getAuthInfo(function (api) {
      request(
        {
          method: "DELETE",
          uri: api.storageURL + "?bulk-delete",
          headers: {
            "Content-type": "text/plain",
            Accept: "application/json",
            "X-Auth-Token": api.authToken,
          },
          body: deleteObjectList,
        },
        function (error, response, body) {
          if (response.statusCode === 200) {
            round++;

            if (
              JSON.parse(body).Errors[0] &&
              JSON.parse(body).Errors[0][1] === 401
            ) {
              authenticate(function (authInfoFresh) {
                request(
                  {
                    method: "DELETE",
                    uri: api.storageURL + "?bulk-delete",
                    headers: {
                      "Content-type": "text/plain",
                      Accept: "application/json",
                      "X-Auth-Token": authInfoFresh.authToken,
                    },
                    body: deleteObjectList,
                  },
                  function (error, response, body) {
                    if (body) {
                    }

                    if (response.statusCode === 200) {
                      //console.log(JSON.parse(body));
                      if (round < maxRound) {
                        deleteCloudFileContainerObjectsInChunk(
                          containerName,
                          objectsNames,
                          maxObjectsDeleteLimit,
                          maxRound,
                          round,
                          callback
                        );
                      } else if (round === maxRound) {
                        callback(2);
                      } else {
                        callback(null);
                      }
                    } else if (
                      response.statusCode === 400 ||
                      response.statusCode === 502
                    ) {
                      //console.log('Server Error');
                      callback(null);
                    } else {
                      callback(null);
                    }
                  }
                );
              });
            } else if (round < maxRound) {
              //console.log(JSON.parse(body));
              deleteCloudFileContainerObjectsInChunk(
                containerName,
                objectsNames,
                maxObjectsDeleteLimit,
                maxRound,
                round,
                callback
              );
            } else if (round === maxRound) {
              callback(1);
            } else {
              callback(null);
            }
          } else if (
            response.statusCode === 400 ||
            response.statusCode === 502
          ) {
            //console.log('Server Error');
            callback(null);
          } else {
            callback(null);
          }
        }
      );
    });
  } else {
    callback(null);
  }
}

function deleteMultipleObjectInCloudFileContainer(
  containerName,
  objectsNames,
  maxObjectsDeleteLimit,
  callback
) {
  if (!maxObjectsDeleteLimit || maxObjectsDeleteLimit > 10000) {
    maxObjectsDeleteLimit = 10000;
  }

  if (containerName && objectsNames && objectsNames.length > 0) {
    var maxRound = Math.ceil(objectsNames.length / maxObjectsDeleteLimit);

    deleteCloudFileContainerObjectsInChunk(
      containerName,
      objectsNames,
      maxObjectsDeleteLimit,
      maxRound,
      null,
      function (statusCode) {
        callback(statusCode);
        containerName = null;
        objectsNames = null;
        maxObjectsDeleteLimit = null;
      }
    );
  } else {
    callback(null);
  }
}

function deleteAllObjectsInCloudFileContainer(containerName, callback) {
  if (containerName && containerName.length > 0) {
    encodeContainerName(containerName, function (encodedContainerName) {
      getCloudFileContainerObjects(
        encodedContainerName,
        null,
        function (objects) {
          if (objects && objects.length > 0) {
            var objectsCount = parseInt(objects.length, 10);
            var maxObjectsDeleteLimit = 10000;
            var maxRound = Math.ceil(objectsCount / maxObjectsDeleteLimit);

            var objectsNames = [];

            for (var i = 0; i < objects.length; i++) {
              objectsNames.push(objects[i].name);
            }

            encodeObjectNames(objectsNames, function (encodedObjectNames) {
              deleteCloudFileContainerObjectsInChunk(
                encodedContainerName,
                encodedObjectNames,
                maxObjectsDeleteLimit,
                maxRound,
                null,
                function (statusCode) {
                  callback(statusCode);

                  encodedContainerName = null;
                  encodedObjectNames = null;
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  } else {
    callback(null);
  }
}

//--------------------------------------------------------------------------------

// Delete containers

function deleteCloudFileContainers(containerNames, callback) {
  if (typeof containerNames === "string" || containerNames instanceof String) {
    containerNames = [containerNames];
  }

  if (containerNames && containerNames.length > 0) {
    async.waterfall(
      [
        function (callback) {
          async.map(
            containerNames,
            function (containerName, callback) {
              deleteAllObjectsInCloudFileContainer(containerName, function () {
                callback(null, containerName);
              });
            },
            function (err, results) {
              callback(null, results);
            }
          );
        },
        function (emptyContainerNames, callback) {
          encodeContainerNames(
            emptyContainerNames,
            function (encodedContainerNames) {
              callback(null, encodedContainerNames);
            }
          );
        },
        function (encodedContainerNames, callback) {
          async.map(
            encodedContainerNames,
            function (containerName, callback) {
              getAuthInfo(function (api) {
                request(
                  {
                    method: "DELETE",
                    uri: api.storageURL + "/" + containerName,
                    headers: {
                      "X-Auth-Token": api.authToken,
                    },
                  },
                  function (error, response, body) {
                    if (body) {
                    }

                    if (response.statusCode === 204) {
                      callback(null, 1);
                    } else if (response.statusCode === 404) {
                      //console.log('Container '+containerName+' not exist A');
                      callback(null, null);
                    } else if (response.statusCode === 409) {
                      //console.log('Container '+containerName+' not empty A');
                      callback(null, 0);
                    } else if (response.statusCode === 401) {
                      authenticate(function (authInfoFresh) {
                        request(
                          {
                            method: "DELETE",
                            uri: api.storageURL + "/" + containerName,
                            headers: {
                              "X-Auth-Token": authInfoFresh.authToken,
                            },
                          },
                          function (error, response, body) {
                            if (body) {
                            }

                            if (response.statusCode === 204) {
                              callback(null, 1);
                            } else if (response.statusCode === 404) {
                              //console.log('Container '+containerName+' not exist B');
                              callback(null, null);
                            } else if (response.statusCode === 409) {
                              //console.log('Container '+containerName+' not empty B');
                              callback(null, 0);
                            } else {
                              callback(null, null);
                            }
                          }
                        );
                      });
                    } else {
                      callback(null, null);
                    }
                  }
                );
              });
            },
            function (err, results) {
              callback(null, results);
            }
          );
        },
      ],
      function (err, result) {
        callback(result);
      }
    );
  }
}

//--------------------------------------------------------------------------------

// Create update object

function getHash(algorithmName, filePath, callback) {
  var sum = crypto.createHash(algorithmName);

  var readStream = fs.ReadStream(filePath);
  readStream.on("data", function (d) {
    sum.update(d);
  });
  readStream.on("end", function () {
    callback(sum.digest("hex"));
  });
}

function createUpdateCloudFileObjects(
  filePath,
  containerName,
  contentType,
  metaData,
  expiredDate,
  callback
) {
  if (
    filePath &&
    containerName &&
    filePath.length > 0 &&
    containerName.length > 0
  ) {
    async.waterfall(
      [
        function (callback) {
          fs.exists(filePath, function (exists) {
            if (exists) {
              callback(null);
            } else {
              callback(filePath + " does not exist");
            }
          });
        },
        function (callback) {
          getHash("md5", filePath, function (hash) {
            callback(null, hash);
          });
        },
        function (hash, callback) {
          encodeContainerName(containerName, function (encodedContainerName) {
            callback(null, hash, encodedContainerName);
          });
        },
        function (hash, encodedContainerName, callback) {
          getAuthInfo(function (api) {
            var headerValues = {};
            if (metaData) {
              headerValues = metaData;
            }
            headerValues["X-Auth-Token"] = api.authToken;
            headerValues["Content-type"] = contentType;
            headerValues.ETag = hash;
            headerValues["Transfer-Encoding"] = "chunked";

            if (expiredDate) {
              headerValues["X-Delete-At"] = Math.floor(
                expiredDate.getTime() / 1000.0
              ).toString();
            }

            fs.createReadStream(filePath).pipe(
              request(
                {
                  method: "PUT",
                  uri:
                    api.storageURL +
                    "/" +
                    encodedContainerName +
                    "/" +
                    path.basename(filePath),
                  headers: headerValues,
                },
                function (error, response, body) {
                  if (body) {
                  }

                  //console.log('A '+response.statusCode);
                  //console.log(body);

                  if (response && response.statusCode === 201) {
                    callback(null, 201, hash, encodedContainerName);
                  } else if (response && response.statusCode === 404) {
                    callback(null, 404, hash, encodedContainerName);
                  } else if (response && response.statusCode === 401) {
                    callback(null, 401, hash, encodedContainerName);
                  } else {
                    if (response) {
                      callback(response.statusCode);
                    } else {
                      callback("No response");
                    }
                  }
                }
              )
            );
          });
        },
        function (statusCode, hash, encodedContainerName, callback) {
          if (statusCode === 404) {
            encodeContainerName(containerName, function (encodedContainerName) {
              createCloudFileContainer(
                encodedContainerName,
                metaData,
                function (success) {
                  if (success) {
                    callback(null, statusCode, hash, encodedContainerName);
                  } else {
                    callback(404);
                  }
                }
              );
            });
          } else {
            callback(null, statusCode, hash, encodedContainerName);
          }
        },
        function (statusCode, hash, encodedContainerName, callback) {
          if (statusCode === 401 || statusCode === 404) {
            authenticate(function (authInfoFresh) {
              if (authInfoFresh && authInfoFresh.authToken) {
                var headerValues = {};
                if (metaData) {
                  headerValues = metaData;
                }
                headerValues["X-Auth-Token"] = authInfoFresh.authToken;
                headerValues["Content-type"] = contentType;
                headerValues.ETag = hash;
                headerValues["Transfer-Encoding"] = "chunked";

                if (expiredDate) {
                  headerValues["X-Delete-At"] = Math.floor(
                    expiredDate.getTime() / 1000.0
                  ).toString();
                }

                fs.createReadStream(filePath).pipe(
                  request(
                    {
                      method: "PUT",
                      uri:
                        authInfoFresh.storageURL +
                        "/" +
                        encodedContainerName +
                        "/" +
                        path.basename(filePath),
                      headers: headerValues,
                    },
                    function (error, response, body) {
                      if (body) {
                      }

                      //console.log('B '+response.statusCode);
                      //console.log(body);

                      if (response && response.statusCode === 201) {
                        callback(null, 1);
                      } else {
                        if (response) {
                          callback(response.statusCode);
                        } else {
                          callback("No response");
                        }
                      }
                    }
                  )
                );
              } else {
                callback("Authentication failed");
              }
            });
          } else if (statusCode === 201) {
            callback(null, 1);
          } else {
            callback(null);
          }
        },
      ],
      function (err, statusCode) {
        if (err) {
          //console.log(err);
          callback(null);
        } else if (statusCode) {
          callback(statusCode);
        } else {
          callback(null);
        }
      }
    );
  } else {
    callback(null);
  }
}

//--------------------------------------------------------------------------------

// Get object meta data

function getObjectDetails(containerName, objectName, callback) {
  if (
    containerName &&
    objectName &&
    containerName.length > 0 &&
    objectName.length > 0
  ) {
    encodeContainerName(containerName, function (encodedContainerName) {
      encodeObjectName(objectName, function (encodedObjectName) {
        getAuthInfo(function (api) {
          request(
            {
              method: "HEAD",
              uri:
                api.storageURL +
                "/" +
                encodedContainerName +
                "/" +
                encodedObjectName,
              headers: {
                "X-Auth-Token": api.authToken,
              },
            },
            function (error, response, body) {
              if (body) {
              }

              //console.log('A '+response.statusCode);

              if (response && response.statusCode === 200) {
                var objectDetails = {};

                objectDetails.contentSize = response.headers["content-length"];
                objectDetails.contentType = response.headers["content-type"];
                objectDetails.etag = response.headers.etag;
                objectDetails.timeStamp = new Date(
                  parseInt(response.headers["x-timestamp"], 10) * 1000,
                  10
                );
                objectDetails.lastModified = new Date(
                  response.headers["last-modified"]
                );

                if (response.headers["x-delete-at"]) {
                  objectDetails.expiredDate = new Date(
                    parseInt(response.headers["x-delete-at"], 10) * 1000
                  );
                }

                var metaTag;
                var metaHeaderPrefix = "x-object-meta-";

                for (var x in response.headers) {
                  if (x.indexOf(metaHeaderPrefix) !== -1) {
                    if (!metaTag) {
                      metaTag = {};
                    }
                    metaTag[
                      x.substr(
                        x.indexOf(metaHeaderPrefix) + metaHeaderPrefix.length,
                        x.length - metaHeaderPrefix.length
                      )
                    ] = response.headers[x];
                  }
                }

                if (metaTag) {
                  objectDetails.metaTag = metaTag;
                }

                callback(objectDetails);
              } else if (response && response.statusCode === 401) {
                authenticate(function (authInfoFresh) {
                  if (authInfoFresh && authInfoFresh.authToken) {
                    request(
                      {
                        method: "HEAD",
                        uri:
                          api.storageURL +
                          "/" +
                          encodedContainerName +
                          "/" +
                          encodedObjectName,
                        headers: {
                          "X-Auth-Token": authInfoFresh.authToken,
                        },
                      },
                      function (error, response, body) {
                        //console.log('B '+response.statusCode);

                        if (body) {
                        }

                        if (response.statusCode === 200) {
                          var objectDetails = {};

                          objectDetails.contentSize =
                            response.headers["content-length"];
                          objectDetails.contentType =
                            response.headers["content-type"];
                          objectDetails.etag = response.headers.etag;
                          objectDetails.timeStamp = new Date(
                            parseInt(response.headers["x-timestamp"], 10) * 1000
                          );
                          objectDetails.lastModified = new Date(
                            response.headers["last-modified"]
                          );

                          if (response.headers["x-delete-at"]) {
                            objectDetails.expiredDate = new Date(
                              parseInt(response.headers["x-delete-at"], 10) *
                                1000
                            );
                          }

                          var metaTag;
                          var metaHeaderPrefix = "x-object-meta-";

                          for (var x in response.headers) {
                            if (x.indexOf(metaHeaderPrefix) !== -1) {
                              if (!metaTag) {
                                metaTag = {};
                              }
                              metaTag[
                                x.substr(
                                  x.indexOf(metaHeaderPrefix) +
                                    metaHeaderPrefix.length,
                                  x.length - metaHeaderPrefix.length
                                )
                              ] = response.headers[x];
                            }
                          }

                          if (metaTag) {
                            objectDetails.metaTag = metaTag;
                          }

                          callback(objectDetails);
                        } else {
                          callback(null);
                        }
                      }
                    );
                  } else {
                    callback(null);
                  }
                });
              } else {
                callback(null);
              }
            }
          );
        });
      });
    });
  } else {
    callback(null);
  }
}

//--------------------------------------------------------------------------------

// Get object meta data

// Set, update : X-Object-Meta-Book: 'Hello world'
// Delete      : X-Remove-Object-Meta-Name: foo

function updateCloudFileObjectMetaData(
  containerName,
  objectName,
  metaData,
  expiredDate,
  callback
) {
  if (
    containerName &&
    objectName &&
    containerName.length > 0 &&
    objectName.length > 0
  ) {
    encodeContainerName(containerName, function (encodedContainerName) {
      encodeObjectName(objectName, function (encodedObjectName) {
        if (metaData || expiredDate) {
          getAuthInfo(function (api) {
            var headerValues = {};
            if (metaData) {
              headerValues = metaData;
            }
            headerValues["X-Auth-Token"] = api.authToken;

            if (expiredDate) {
              headerValues["X-Delete-At"] = Math.floor(
                expiredDate.getTime() / 1000.0
              ).toString();
            }

            request(
              {
                method: "POST",
                uri:
                  api.storageURL +
                  "/" +
                  encodedContainerName +
                  "/" +
                  encodedObjectName,
                headers: headerValues,
              },
              function (error, response, body) {
                if (body) {
                }

                //console.log('A '+response.statusCode);

                if (response.statusCode === 202) {
                  callback(1);
                } else if (response.statusCode === 404) {
                  callback(null);
                } else if (response.statusCode === 401) {
                  authenticate(function (authInfoFresh) {
                    headerValues["X-Auth-Token"] = authInfoFresh.authToken;

                    request(
                      {
                        method: "POST",
                        uri:
                          api.storageURL +
                          "/" +
                          encodedContainerName +
                          "/" +
                          encodedObjectName,
                        headers: headerValues,
                      },
                      function (error, response, body) {
                        if (body) {
                        }

                        if (response.statusCode === 202) {
                          //console.log('B '+response.statusCode);

                          callback(1);
                        } else if (response.statusCode === 404) {
                          callback(null);
                        } else {
                          callback(null);
                        }
                      }
                    );
                  });
                } else {
                  callback(null);
                }
              }
            );
          });
        }
      });
    });
  } else {
    callback(null);
  }
}

//--------------------------------------------------------------------------------

// Copy object

function copyCloudFileObject(
  fromContainerName,
  fromObjectName,
  toContainerName,
  toObjectName,
  metaData,
  callback
) {
  if (fromContainerName && fromObjectName && toContainerName && toObjectName) {
    if (
      fromContainerName.length > 0 &&
      fromObjectName.length > 0 &&
      toContainerName.length > 0 &&
      toObjectName.length > 0
    ) {
      async.waterfall(
        [
          function (callback) {
            encodeContainerName(
              fromContainerName,
              function (encodedFromContainerName) {
                callback(null, encodedFromContainerName);
              }
            );
          },
          function (encodedFromContainerName, callback) {
            encodeObjectName(fromObjectName, function (encodedFromObjectName) {
              callback(null, encodedFromContainerName, encodedFromObjectName);
            });
          },
          function (encodedFromContainerName, encodedFromObjectName, callback) {
            encodeContainerName(
              toContainerName,
              function (encodedToContainerName) {
                callback(
                  null,
                  encodedFromContainerName,
                  encodedFromObjectName,
                  encodedToContainerName
                );
              }
            );
          },
          function (
            encodedFromContainerName,
            encodedFromObjectName,
            encodedToContainerName,
            callback
          ) {
            encodeObjectName(toObjectName, function (encodedToObjectName) {
              callback(
                null,
                encodedFromContainerName,
                encodedFromObjectName,
                encodedToContainerName,
                encodedToObjectName
              );
            });
          },
          function (
            encodedFromContainerName,
            encodedFromObjectName,
            encodedToContainerName,
            encodedToObjectName,
            callback
          ) {
            getAuthInfo(function (api) {
              var headerValues = {};
              if (metaData) {
                headerValues = metaData;
              }
              headerValues["X-Auth-Token"] = api.authToken;
              headerValues.Destination =
                "/" + encodedToContainerName + "/" + encodedToObjectName;

              request(
                {
                  method: "COPY",
                  uri:
                    api.storageURL +
                    "/" +
                    encodedFromContainerName +
                    "/" +
                    encodedFromObjectName,
                  headers: headerValues,
                },
                function (error, response, body) {
                  //console.log('A '+response.statusCode);

                  if (response.statusCode === 201) {
                    callback(null, 1);
                  } else if (response.statusCode === 404) {
                    callback(body);
                  } else if (response.statusCode === 401) {
                    authenticate(function (authInfoFresh) {
                      headerValues["X-Auth-Token"] = authInfoFresh.authToken;

                      request(
                        {
                          method: "COPY",
                          uri:
                            api.storageURL +
                            "/" +
                            encodedFromContainerName +
                            "/" +
                            encodedFromObjectName,
                          headers: headerValues,
                        },
                        function (error, response, body) {
                          //console.log('B '+response.statusCode);

                          if (response.statusCode === 201) {
                            callback(null, 1);
                          } else if (response.statusCode === 404) {
                            callback(body);
                          } else {
                            callback(null);
                          }
                        }
                      );
                    });
                  } else {
                    callback(null);
                  }
                }
              );
            });
          },
        ],
        function (err, result) {
          if (err) {
            //console.log(err);
            callback(null);
          } else if (result) {
            callback(result);
          } else {
            callback(null);
          }
        }
      );
    } else {
      callback(null);
    }
  } else {
    callback(null);
  }
}

// Move object

function moveCloudFileObject(
  fromContainerName,
  fromObjectName,
  toContainerName,
  toObjectName,
  metaData,
  callback
) {
  copyCloudFileObject(
    fromContainerName,
    fromObjectName,
    toContainerName,
    toObjectName,
    metaData,
    function (statusCode) {
      if (statusCode === 1) {
        encodeContainerName(fromContainerName, function (encodedContainerName) {
          encodeObjectName(fromObjectName, function (encodedObjectName) {
            deleteSingleObjectInCloudFileContainer(
              encodedContainerName,
              encodedObjectName,
              function (statusCode) {
                callback(statusCode);
              }
            );
          });
        });
      } else {
        callback(statusCode);
      }
    }
  );
}

// Rename object

function renameUpdateCloudFileObject(
  containerName,
  fromObjectName,
  toObjectName,
  metaData,
  callback
) {
  copyCloudFileObject(
    containerName,
    fromObjectName,
    containerName,
    toObjectName,
    metaData,
    function (statusCode) {
      if (statusCode === 1) {
        encodeContainerName(containerName, function (encodedContainerName) {
          encodeObjectName(fromObjectName, function (encodedObjectName) {
            deleteSingleObjectInCloudFileContainer(
              encodedContainerName,
              encodedObjectName,
              function (statusCode) {
                callback(statusCode);
              }
            );
          });
        });
      } else {
        callback(statusCode);
      }
    }
  );
}

//--------------------------------------------------------------------------------

// Download object

function downloadCloudFileObject(
  containerName,
  objectName,
  savePath,
  callback
) {
  if (
    containerName &&
    objectName &&
    containerName.length > 0 &&
    objectName.length > 0
  ) {
    encodeContainerName(containerName, function (encodedContainerName) {
      encodeObjectName(objectName, function (encodedObjectName) {
        getAuthInfo(function (api) {
          savePath = path.join(savePath, objectName);

          var localStream = fs.createWriteStream(savePath);

          var out = request({
            method: "GET",
            uri:
              api.storageURL +
              "/" +
              encodedContainerName +
              "/" +
              encodedObjectName,
            headers: {
              "X-Auth-Token": api.authToken,
            },
          });

          out.setMaxListeners(0);

          //************************************

          out.on("response", function (resp) {
            if (resp.statusCode === 200 || resp.statusCode === 304) {
              out.pipe(localStream);

              localStream.on("close", function () {
                callback(1);
              });

              localStream.on("error", function () {
                fs.unlink(savePath, function (err) {
                  if (err) {
                    callback(err);
                  } else {
                    //console.log('A successfully deleted '+savePath);
                    callback(null);
                  }
                });
              });
            } //************************************
            else if (resp.statusCode === 401) {
              //console.log('Authentication error');

              authenticate(function (authInfoFresh) {
                var outB = request({
                  method: "GET",
                  uri:
                    api.storageURL +
                    "/" +
                    encodedContainerName +
                    "/" +
                    encodedObjectName,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                  },
                });

                outB.setMaxListeners(0);

                outB.on("response", function (resp) {
                  if (resp.statusCode === 200 || resp.statusCode === 304) {
                    outB.pipe(localStream);

                    localStream.on("close", function () {
                      callback(1);
                    });

                    localStream.on("error", function () {
                      fs.unlink(savePath, function (err) {
                        if (err) {
                          callback(err);
                        } else {
                          //console.log('B successfully deleted '+savePath);
                          callback(null);
                        }
                      });
                    });
                  } else {
                    fs.unlink(savePath, function (err) {
                      if (err) {
                        callback(err);
                      } else {
                        //console.log("B No file found at url");
                        callback(null);
                      }
                    });
                  }
                });
              });
            } //************************************
            else {
              fs.unlink(savePath, function (err) {
                if (err) {
                  callback(err);
                } else {
                  //console.log("A No file found at url");
                  callback(null);
                }
              });
            }
          });
        });
      });
    });
  } else {
    callback(null);
  }
}

//--------------------------------------------------------------------------------

// List CDN-Enabled Container

//--------------------------------------------------------------------------------

// Get list of CDN-Enabled container in json, max 10000 containers for each fetch

function getCDNEnabledContainerListInChunk(
  maxContainersFetchLimit,
  maxRound,
  round,
  marker,
  containers,
  callback
) {
  if (!maxContainersFetchLimit || maxContainersFetchLimit > 10000) {
    maxContainersFetchLimit = 10000;
  }
  if (!containers) {
    containers = [];
  }
  if (!round) {
    round = 0;
  }

  if (round < maxRound) {
    getAuthInfo(function (api) {
      var url =
        api.cdnURL +
        "?format=json" +
        "&limit=" +
        maxContainersFetchLimit +
        "&enabled_only=true";
      if (marker) {
        url = url + "&marker=" + encodeMarkerName(marker);
      }

      request(
        {
          method: "GET",
          uri: url,
          headers: {
            "X-Auth-Token": api.authToken,
          },
        },
        function (error, response, body) {
          if (response.statusCode === 200) {
            var containerArray = JSON.parse(body);

            if (containerArray.length > 0) {
              containers = containers.concat(containerArray);

              round++;
              if (round < maxRound) {
                marker = null;
                var lastContainerName =
                  containerArray[containerArray.length - 1].name;
                getContainerListInChunk(
                  maxContainersFetchLimit,
                  maxRound,
                  round,
                  lastContainerName,
                  containers,
                  callback
                );
              } else if (round === maxRound) {
                callback(containers);
                marker = null;
              }
            } else {
              callback(null);
            }
          } else if (response.statusCode === 204) {
            callback(null);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              request(
                {
                  method: "GET",
                  uri: url,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                  },
                },
                function (error, response, body) {
                  if (response.statusCode === 200) {
                    var containerArray = JSON.parse(body);

                    if (containerArray.length > 0) {
                      containers = containers.concat(containerArray);

                      round++;
                      if (round < maxRound) {
                        marker = null;
                        var lastContainerName =
                          containerArray[containerArray.length - 1].name;
                        getContainerListInChunk(
                          maxContainersFetchLimit,
                          maxRound,
                          round,
                          lastContainerName,
                          containers,
                          callback
                        );
                      } else if (round === maxRound) {
                        callback(containers);
                        marker = null;
                      }
                    } else {
                      callback(null);
                    }
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  } else {
    callback(null);
  }
}

function getCDNEnabledContainerList(maxContainersFetchLimit, callback) {
  if (!maxContainersFetchLimit || maxContainersFetchLimit > 10000) {
    maxContainersFetchLimit = 10000;
  }

  getAccountDetails(function (accountDetails) {
    if (accountDetails) {
      var containersCount = parseInt(accountDetails.containerCount, 10);
      var maxRound = Math.ceil(containersCount / maxContainersFetchLimit);

      getCDNEnabledContainerListInChunk(
        maxContainersFetchLimit,
        maxRound,
        null,
        null,
        null,
        function (containerObjects) {
          callback(containerObjects);
        }
      );
    } else {
      callback(null);
    }
  });
}

//--------------------------------------------------------------------------------

// Get CDN-Enabled container details

function getCDNEnabledContainerDetails(containerName, callback) {
  encodeContainerName(containerName, function (encodedContainerName) {
    getAuthInfo(function (api) {
      request(
        {
          method: "HEAD",
          uri: api.cdnURL + "/" + encodedContainerName,
          headers: {
            "X-Auth-Token": api.authToken,
          },
        },
        function (error, response, body) {
          if (body) {
          }

          //console.log('A '+response.statusCode);

          if (response.statusCode === 204) {
            var containerDetails = response.headers;

            delete containerDetails.date;
            delete containerDetails["x-trans-id"];
            delete containerDetails["content-type"];
            delete containerDetails["content-length"];
            delete containerDetails.connection;

            callback(containerDetails);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              request(
                {
                  method: "HEAD",
                  uri: api.cdnURL + "/" + encodedContainerName,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                  },
                },
                function (error, response, body) {
                  if (body) {
                  }
                  //console.log('B '+response.statusCode);

                  if (response.statusCode === 204) {
                    var containerDetails = response.headers;

                    delete containerDetails.date;
                    delete containerDetails["x-trans-id"];
                    delete containerDetails["content-type"];
                    delete containerDetails["content-length"];
                    delete containerDetails.connection;

                    callback(containerDetails);
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  });
}

//--------------------------------------------------------------------------------

// CDN-Enable a container

function executeCDNEnableContainer(containerName, ttl, callback) {
  encodeContainerName(containerName, function (encodedContainerName) {
    if (!ttl) {
      ttl = 3600;
    }

    getAuthInfo(function (api) {
      request(
        {
          method: "PUT",
          uri: api.cdnURL + "/" + encodedContainerName,
          headers: {
            "X-Auth-Token": api.authToken,
            "X-CDN-Enabled": "TRUE",
            "X-TTL": ttl.toString(),
          },
        },
        function (error, response, body) {
          if (body) {
          }
          //console.log('A '+response.statusCode);

          if (response.statusCode === 201 || response.statusCode === 202) {
            var containerDetails = response.headers;

            delete containerDetails.date;
            delete containerDetails["x-trans-id"];
            delete containerDetails["content-type"];
            delete containerDetails["content-length"];
            delete containerDetails.connection;

            callback(containerDetails);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              request(
                {
                  method: "PUT",
                  uri: api.cdnURL + "/" + encodedContainerName,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                    "X-CDN-Enabled": "TRUE",
                    "X-TTL": ttl.toString(),
                  },
                },
                function (error, response, body) {
                  if (body) {
                  }
                  //console.log('B '+response.statusCode);

                  if (
                    response.statusCode === 201 ||
                    response.statusCode === 202
                  ) {
                    var containerDetails = response.headers;

                    delete containerDetails.date;
                    delete containerDetails["x-trans-id"];
                    delete containerDetails["content-type"];
                    delete containerDetails["content-length"];
                    delete containerDetails.connection;

                    callback(containerDetails);
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  });
}

//--------------------------------------------------------------------------------

// CDN-Disable a container

function executeCDNDisableContainer(containerName, callback) {
  encodeContainerName(containerName, function (encodedContainerName) {
    getAuthInfo(function (api) {
      request(
        {
          method: "PUT",
          uri: api.cdnURL + "/" + encodedContainerName,
          headers: {
            "X-Auth-Token": api.authToken,
            "X-CDN-Enabled": "FALSE",
            "X-TTL": "900",
          },
        },
        function (error, response, body) {
          if (body) {
          }
          //console.log('A '+response.statusCode);

          if (response.statusCode === 201 || response.statusCode === 202) {
            var containerDetails = response.headers;

            delete containerDetails.date;
            delete containerDetails["x-trans-id"];
            delete containerDetails["content-type"];
            delete containerDetails["content-length"];
            delete containerDetails.connection;

            callback(containerDetails);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              request(
                {
                  method: "PUT",
                  uri: api.cdnURL + "/" + encodedContainerName,
                  headers: {
                    "X-Auth-Token": authInfoFresh.authToken,
                    "X-CDN-Enabled": "FALSE",
                    "X-TTL": "900",
                  },
                },
                function (error, response, body) {
                  if (body) {
                  }
                  //console.log('B '+response.statusCode);

                  if (
                    response.statusCode === 201 ||
                    response.statusCode === 202
                  ) {
                    var containerDetails = response.headers;

                    delete containerDetails.date;
                    delete containerDetails["x-trans-id"];
                    delete containerDetails["content-type"];
                    delete containerDetails["content-length"];
                    delete containerDetails.connection;

                    callback(containerDetails);
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  });
}

//--------------------------------------------------------------------------------

// CDN-Disable a container

function modifyCDNContainerAttributes(
  containerName,
  ttl,
  cdnEnable,
  logRetention,
  callback
) {
  encodeContainerName(containerName, function (encodedContainerName) {
    getAuthInfo(function (api) {
      var headerValues = {};
      headerValues["X-Auth-Token"] = api.authToken;

      if (ttl) {
        headerValues["X-TTL"] = ttl.toString();
      }
      if (cdnEnable) {
        headerValues["X-CDN-Enabled"] = cdnEnable;
      }
      if (logRetention) {
        headerValues["X-Log-Retention"] = logRetention;
      }

      request(
        {
          method: "POST",
          uri: api.cdnURL + "/" + encodedContainerName,
          headers: headerValues,
        },
        function (error, response, body) {
          if (body) {
          }
          //console.log('A '+response.statusCode);

          if (
            response.statusCode === 201 ||
            response.statusCode === 202 ||
            response.statusCode === 204
          ) {
            var containerDetails = response.headers;

            delete containerDetails.date;
            delete containerDetails["x-trans-id"];
            delete containerDetails["content-type"];
            delete containerDetails["content-length"];
            delete containerDetails.connection;

            callback(containerDetails);
          } else if (response.statusCode === 401) {
            authenticate(function (authInfoFresh) {
              headerValues["X-Auth-Token"] = authInfoFresh.authToken;

              request(
                {
                  method: "POST",
                  uri: api.cdnURL + "/" + encodedContainerName,
                  headers: headerValues,
                },
                function (error, response, body) {
                  if (body) {
                  }

                  //console.log('B '+response.statusCode);

                  if (
                    response.statusCode === 201 ||
                    response.statusCode === 202 ||
                    response.statusCode === 204
                  ) {
                    var containerDetails = response.headers;

                    delete containerDetails.date;
                    delete containerDetails["x-trans-id"];
                    delete containerDetails["content-type"];
                    delete containerDetails["content-length"];
                    delete containerDetails.connection;

                    callback(containerDetails);
                  } else {
                    callback(null);
                  }
                }
              );
            });
          } else {
            callback(null);
          }
        }
      );
    });
  });
}

//--------------------------------------------------------------------------------

// Export Module functions

exports.authDetails = function authDetails(callback) {
  getAuthInfo(function (api) {
    callback(api);
  });
};

exports.accountDetails = function accountDetails(callback) {
  getAccountDetails(function (accountDetails) {
    callback(accountDetails);
  });
};

exports.containerDetails = function containerDetails(containerName, callback) {
  if (containerName && containerName.length > 0) {
    encodeContainerName(containerName, function (encodedContainerName) {
      getContainerDetails(encodedContainerName, function (containerDetails) {
        callback(containerDetails);
      });
    });
  } else {
    callback(null);
  }
};

exports.containerList = function containerList(callback) {
  getContainerList(null, function (containerList) {
    callback(containerList);
  });
};

exports.createContainer = function createContainer(
  containerName,
  metaData,
  callback
) {
  if (containerName && containerName.length > 0) {
    encodeContainerName(containerName, function (encodedContainerName) {
      createCloudFileContainer(
        encodedContainerName,
        metaData,
        function (statusCode) {
          callback(statusCode);
        }
      );
    });
  } else {
    callback(null);
  }
};

exports.setUpdateDeleteContainerMetaData =
  function setUpdateDeleteContainerMetaDat(containerName, metaData, callback) {
    if (metaData && containerName && containerName.length > 0) {
      encodeContainerName(containerName, function (encodedContainerName) {
        setUpdateDeleteCloudFileContainerMetaData(
          encodedContainerName,
          metaData,
          function (statusCode) {
            callback(statusCode);
          }
        );
      });
    } else {
      callback(null);
    }
  };

exports.getContainerObjects = function getContainerObjects(
  containerName,
  callback
) {
  if (containerName && containerName.length > 0) {
    encodeContainerName(containerName, function (encodedContainerName) {
      getCloudFileContainerObjects(
        encodedContainerName,
        null,
        function (objectsList) {
          callback(objectsList);
        }
      );
    });
  } else {
    callback(null);
  }
};

exports.deleteSingleObject = function deleteSingleObject(
  containerName,
  objectName,
  callback
) {
  if (containerName && objectName) {
    if (containerName.length > 0 && objectName.length > 0) {
      encodeContainerName(containerName, function (encodedContainerName) {
        encodeObjectName(objectName, function (encodedObjectName) {
          deleteSingleObjectInCloudFileContainer(
            encodedContainerName,
            encodedObjectName,
            function (statusCode) {
              callback(statusCode);
            }
          );
        });
      });
    } else {
      callback(null);
    }
  } else {
    callback(null);
  }
};

exports.deleteMultipleObjects = function deleteMultipleObjects(
  containerName,
  objectNames,
  callback
) {
  if (containerName && objectNames) {
    if (containerName.length > 0 && objectNames.length > 0) {
      encodeContainerName(containerName, function (encodedContainerName) {
        encodeObjectNames(objectNames, function (encodedObjectNames) {
          deleteMultipleObjectInCloudFileContainer(
            encodedContainerName,
            encodedObjectNames,
            null,
            function (statusCode) {
              callback(statusCode);
            }
          );
        });
      });
    } else {
      callback(null);
    }
  } else {
    callback(null);
  }
};

exports.deleteAllObjectsInContainer = function deleteAllObjectsInContainer(
  containerName,
  callback
) {
  deleteAllObjectsInCloudFileContainer(containerName, function (statusCode) {
    callback(statusCode);
  });
};

exports.deleteContainers = function deleteContainers(containerNames, callback) {
  deleteCloudFileContainers(containerNames, function (statusCode) {
    callback(statusCode);
  });
};

exports.createUpdateObject = function createUpdateObject(
  filePath,
  containerName,
  contentType,
  metaData,
  expiredDate,
  callback
) {
  createUpdateCloudFileObjects(
    filePath,
    containerName,
    contentType,
    metaData,
    expiredDate,
    function (statusCode) {
      callback(statusCode);
    }
  );
};

exports.objectDetails = function objectDetails(
  containerName,
  objectName,
  callback
) {
  getObjectDetails(containerName, objectName, function (metaData) {
    callback(metaData);
  });
};

exports.updateObjectMetaData = function updateObjectMetaData(
  containerName,
  objectName,
  metaData,
  expiredDate,
  callback
) {
  updateCloudFileObjectMetaData(
    containerName,
    objectName,
    metaData,
    expiredDate,
    function (statusCode) {
      callback(statusCode);
    }
  );
};

exports.copyObject = function copyObject(
  fromContainerName,
  fromObjectName,
  toContainerName,
  toObjectName,
  metaData,
  callback
) {
  copyCloudFileObject(
    fromContainerName,
    fromObjectName,
    toContainerName,
    toObjectName,
    metaData,
    function (statusCode) {
      callback(statusCode);
    }
  );
};

exports.moveObject = function moveObject(
  fromContainerName,
  fromObjectName,
  toContainerName,
  toObjectName,
  metaData,
  callback
) {
  moveCloudFileObject(
    fromContainerName,
    fromObjectName,
    toContainerName,
    toObjectName,
    metaData,
    function (statusCode) {
      callback(statusCode);
    }
  );
};

exports.renameUpdateObject = function renameUpdateObject(
  containerName,
  fromObjectName,
  toObjectName,
  metaData,
  callback
) {
  renameUpdateCloudFileObject(
    containerName,
    fromObjectName,
    toObjectName,
    metaData,
    function (statusCode) {
      callback(statusCode);
    }
  );
};

exports.downloadObject = function downloadObject(
  containerName,
  objectName,
  savePath,
  callback
) {
  downloadCloudFileObject(
    containerName,
    objectName,
    savePath,
    function (statusCode) {
      callback(statusCode);
    }
  );
};

exports.cdnEnabledContainerList = function cdnEnabledContainerList(callback) {
  getCDNEnabledContainerList(null, function (cdnEnabledContainerList) {
    callback(cdnEnabledContainerList);
  });
};

exports.cdnEnabledContainerDetails = function cdnEnabledContainerDetails(
  containerName,
  callback
) {
  getCDNEnabledContainerDetails(containerName, function (containerDetails) {
    callback(containerDetails);
  });
};

exports.cdnEnableContainer = function cdnEnableContainer(
  containerName,
  TTL,
  callback
) {
  executeCDNEnableContainer(containerName, TTL, function (containerDetails) {
    callback(containerDetails);
  });
};

exports.cdnDisableContainer = function cdnDisableContainer(
  containerName,
  callback
) {
  executeCDNDisableContainer(containerName, function (containerDetails) {
    callback(containerDetails);
  });
};

exports.changeCDNContainerAttributes = function changeCDNContainerAttributes(
  containerName,
  TTL,
  cdnEnable,
  logRetention,
  callback
) {
  if (containerName) {
    if (TTL || cdnEnable || logRetention) {
      modifyCDNContainerAttributes(
        containerName,
        TTL,
        cdnEnable,
        logRetention,
        function (containerDetails) {
          callback(containerDetails);
        }
      );
    } else {
      callback(null);
    }
  } else {
    callback(null);
  }
};
