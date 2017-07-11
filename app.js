var fs = require('fs'),
  async = require('async'),
  argv = require('yargs')
   .usage("CouchDB design document migration")
   .usage('Usage: $0 --dd <design document filename> --db <name of database>')
   .demand(['dd','db'])
   .argv;
  
// get COUCH_URL from the environment
var COUCH_URL = null;
if (typeof process.env.COUCH_URL == "undefined") {
  console.log("Please use environment variable COUCH_URL to indicate URL of your CouchDB/Cloudant");
  console.log("  e.g. export COUCH_URL=http://127.0.0.1:5984");
  process.exit(1);
} else {
  COUCH_URL = process.env.COUCH_URL;
}
var nano = require('nano')( {
  url: COUCH_URL,
  requestDefaults: {
    timeout: 10000,
    headers: {
      'User-Agent': 'couchmigrate',
      'x-cloudant-io-priority': 'low'
    }
  }
});
var db = nano.db.use(argv.db);

var debug = function(err, data) {
  console.log("  err = ", (err)?"true":"");
  console.log("  data = ", JSON.stringify(data));
  console.log("-------------------------------");
};

var copydoc = function(from_id, to_id, cb) {
  var from_doc = null,
    to_doc = null;
  
  async.series([
    // fetch the document we are copying
    function(callback) {
      console.log("## copydoc - Fetching from", from_id);
      db.get(from_id, function(err, data) {
        debug(err, data);
        if (!err) {
          from_doc = data;
        }
        callback(err, data);
      });
    },
    
    // fetch the document we are copying to (if it is there)
    function(callback) {
      console.log("## copydoc - Fetching to", to_id);
      db.get(to_id, function(err, data) {
        debug(err, data);
        if (!err) {
          to_doc = data;
        }
        callback(null, data);
      });
    },
    
    // overwrite the destination
    function(callback) {
      console.log("## copydoc - Writing new to", to_id);
      from_doc._id = to_id;
      if (to_doc) {
        from_doc._rev = to_doc._rev;
      } else { 
        delete from_doc._rev;
      }
      console.log("## copydoc - contents",from_doc);
      db.insert(from_doc, function(err, data) {
        debug(err, data);
        callback(err, data);
      });
    }
  ], cb);
};

var writedoc = function(obj, docid, cb) {
  var preexistingdoc = null;
  async.series([
    function(callback) {
      console.log("## writedoc - Looking for pre-existing", docid);
      db.get(docid, function(err, data) {
        debug(err, data);
        if (!err) {
          preexistingdoc = data;
        }
        callback(null, data);
      });
    },
    function(callback) {
      obj._id = docid;
      if (preexistingdoc) {
        obj._rev = preexistingdoc._rev;
      }
      console.log("## writedoc - Writing doc", obj);
      db.insert(obj, function(err, data) {
        debug(err, data);
        callback(err, data);
      });
    }
  ], cb);
};

var deletedoc = function(docid, cb) {

  console.log("## deletedoc - Looking for docid", docid);
  db.get(docid, function(err, data) {
    debug(err, data);
    if (err) {
      return cb(null, null);
    }
    console.log("## deletedoc - Deleting ", docid, data._rev);
    db.destroy( docid, data._rev, function(err, d) {
      debug(err,d);
      cb(null, null);
    });
  });

};

var clone = function(x) {
  return JSON.parse(JSON.stringify(x));
};

// load the design document
var dd_filename = argv.dd;
fs.readFile(dd_filename, {encoding: "utf8"}, function(err, data) {
  if(err) {
    console.log("Cannot find file", dd_filename);
    process.exit(1);
  }
  
  // this is the whole design document
  try {
    var dd = JSON.parse(data);
  } catch(e) {
    console.log("FAILED to parse file contents as JSON - cannot continue");
    process.exit(1);
  }

  var dd_name = dd._id;
  var original_dd = null;
  var old_dd = null;
  var new_dd = null;
  delete dd._rev;
  var dd_old_name = dd_name + "_OLD";
  var dd_new_name = dd_name + "_NEW";
  
  async.series( [
    // check that the database exists
    function(callback) {
      console.log("## check db exists");
      // if it doesn't we'll get an 'err' and the async process will stop
      nano.db.get(argv.db, function(err, data) {
        debug(err,data);
        callback(err,data);
      });
    },

    // check that the existing view isn't the same as the incoming view
    function(callback) {
      db.get(dd_name, function(err, data) {
        if(err) {
          console.log("!!!");
          return callback(null, null);
        };
        var a = clone(data);
        var b = clone(dd);
        delete a._rev;
        delete a._id;
        delete b._rev;
        delete b._id;
        if(JSON.stringify(a) == JSON.stringify(b)) {
          console.log("** The design document is the same, no need to migrate! **");
          callback(true,null);
        } else {
          callback(null,null);
        }
      });
    },
       
    // copy original design document to _OLD
    function(callback) {
      console.log("## copy original design document to _OLD");
      copydoc(dd_name, dd_old_name, function(err,data) {
        callback(null, null);
      });
    },
    
    // write new design document to _NEW
    function(callback) {
      console.log("## write new design document to _NEW");
      writedoc(dd, dd_new_name, callback)
    },
    
    // wait for the view build to complete, by polling
    function(callback) {
      var hasData = false;

      async.doWhilst(

          function (callback) {
            var name = dd._id.replace(/_design\//,"");
            var v = Object.keys(dd.views)[0];
            console.log("## query ", name, "/", v, "to validate freshness.");

            setTimeout(function() {

              db.view(name, v, { limit:1 }, function(err, data) {
                debug(err, data);

                // on a long view-build this request will timeout and return an 'err'.
                // we should retry until this returns
                hasData = !err && !!data;

                if (err) {
                  // get progress from active tasks
                  nano.request({ path:"_active_tasks"}, function(err, data) {
                    debug(err,data);
                    var progress = 0;
                    var shards = 0;
                    for(var i in data) {
                      var task = data[i];

                      if (task.type === "indexer" && task.design_document === dd_new_name) {
                        shards++;
                        progress = progress + parseInt(task.progress, 10);
                      }
                    }

                    var overallProgress = Math.floor(progress / shards);
                    console.log('## indexing progress:', overallProgress, "%");
                    callback(null, null);
                  });
                }
                else {
                  callback(null, null);
                }
              });
            }, 3000);

          },
          function () { return !hasData; },
          function (err) {
             callback(err, null);
          }
      );
    },
    
    // copy _NEW to live
    function(callback) {
      console.log("## copy _NEW to live", dd_new_name, dd_name);
      copydoc(dd_new_name, dd_name, function(err, data) {
        debug(err,data);
        callback(err,data);
      });
    },
    
    // delete the _OLD view
    function(callback) {
      console.log("## delete the _OLD view", dd_old_name);
      deletedoc(dd_old_name, callback);
    },
    
    // delete the _NEW view
    function(callback) {
      console.log("## delete the _NEW view", dd_new_name);
      deletedoc(dd_new_name, callback);
    }
    
  ], function(err, data) {
    if (err) {
      console.log(err);
    }
    console.log("FINISHED!!!");
  });

});