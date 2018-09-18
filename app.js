const fs = require('fs')
const async = require('async')
const argv = require('yargs')
  .option('designdoc', { alias: 'dd', describe: 'design document filename', demand: true })
  .option('database', { alias: 'db', describe: 'name of database', demand: true })
  .option('url', { alias: 'u', describe: 'URL of CouchDB/Cloudant service', demand: !process.env.COUCH_URL, default: process.env.COUCH_URL })
  .help('help')
  .argv

// get COUCH_URL from the environment
const nano = require('nano')({
  url: argv.url,
  requestDefaults: {
    timeout: 10000,
    headers: {
      'User-Agent': 'couchmigrate',
      'x-cloudant-io-priority': 'low'
    }
  }
})
const db = nano.db.use(argv.database)

const debug = (err, data) => {
  console.log('  err = ', (err) ? 'true' : '')
  console.log('  data = ', JSON.stringify(data))
  console.log('-------------------------------')
}

const copydoc = (fromId, toId, cb) => {
  let fromDoc = null
  let toDoc = null

  async.series([
    // fetch the document we are copying
    function (callback) {
      console.log('## copydoc - Fetching from', fromId)
      db.get(fromId, function (err, data) {
        debug(err, data)
        if (!err) {
          fromDoc = data
        }
        callback(err, data)
      })
    },

    // fetch the document we are copying to (if it is there)
    function (callback) {
      console.log('## copydoc - Fetching to', toId)
      db.get(toId, function (err, data) {
        debug(err, data)
        if (!err) {
          toDoc = data
        }
        callback(null, data)
      })
    },

    // overwrite the destination
    function (callback) {
      console.log('## copydoc - Writing new to', toId)
      fromDoc._id = toId
      if (toDoc) {
        fromDoc._rev = toDoc._rev
      } else {
        delete fromDoc._rev
      }
      console.log('## copydoc - contents', fromDoc)
      db.insert(fromDoc, function (err, data) {
        debug(err, data)
        callback(err, data)
      })
    }
  ], cb)
}

const writedoc = function (obj, docid, cb) {
  var preexistingdoc = null
  async.series([
    function (callback) {
      console.log('## writedoc - Looking for pre-existing', docid)
      db.get(docid, function (err, data) {
        debug(err, data)
        if (!err) {
          preexistingdoc = data
        }
        callback(null, data)
      })
    },
    function (callback) {
      obj._id = docid
      if (preexistingdoc) {
        obj._rev = preexistingdoc._rev
      }
      console.log('## writedoc - Writing doc', obj)
      db.insert(obj, function (err, data) {
        debug(err, data)
        callback(err, data)
      })
    }
  ], cb)
}

const deletedoc = function (docid, cb) {
  console.log('## deletedoc - Looking for docid', docid)
  db.get(docid, function (err, data) {
    debug(err, data)
    if (err) {
      return cb(null, null)
    }
    console.log('## deletedoc - Deleting ', docid, data._rev)
    db.destroy(docid, data._rev, function (err, d) {
      debug(err, d)
      cb(null, null)
    })
  })
}

const clone = function (x) {
  return JSON.parse(JSON.stringify(x))
}

const migrate = function (err, data) {
  if (err) {
    console.log('Cannot find file', ddFilename)
    process.exit(1)
  }

  // this is the whole design document
  let dd
  try {
    dd = JSON.parse(data)
  } catch (e) {
    console.log('FAILED to parse file contents as JSON - cannot continue')
    process.exit(1)
  }

  const ddName = dd._id
  delete dd._rev
  const ddOldName = ddName + '_OLD'
  const ddNewName = ddName + '_NEW'

  async.series([
    // check that the database exists
    function (callback) {
      console.log('## check db exists')
      // if it doesn't we'll get an 'err' and the async process will stop
      nano.db.get(argv.db, function (err, data) {
        debug(err, data)
        callback(err, data)
      })
    },

    // check that the existing view isn't the same as the incoming view
    function (callback) {
      db.get(ddName, function (err, data) {
        if (err) {
          console.log('!!!')
          return callback(null, null)
        }
        const a = clone(data)
        const b = clone(dd)
        delete a._rev
        delete a._id
        delete b._rev
        delete b._id
        if (JSON.stringify(a) === JSON.stringify(b)) {
          console.log('** The design document is the same, no need to migrate! **')
          callback(new Error('design document is the same'), null)
        } else {
          callback(null, null)
        }
      })
    },

    // copy original design document to _OLD
    function (callback) {
      console.log('## copy original design document to _OLD')
      copydoc(ddName, ddOldName, function (err, data) {
        if (err) { }
        callback(null, null)
      })
    },

    // write new design document to _NEW
    function (callback) {
      console.log('## write new design document to _NEW')
      writedoc(dd, ddNewName, callback)
    },

    // wait for the view build to complete, by polling
    function (callback) {
      let hasData = false

      async.doWhilst(

        function (callback) {
          const name = dd._id.replace(/_design\//, '')
          const v = Object.keys(dd.views)[0]
          console.log('## query ', name, '/', v, 'to validate freshness.')

          setTimeout(function () {
            db.view(name, v, { limit: 1 }, function (err, data) {
              debug(err, data)

              // on a long view-build this request will timeout and return an 'err'.
              // we should retry until this returns
              hasData = !err && !!data

              if (err) {
                // get progress from active tasks
                nano.request({ path: '_active_tasks' }, function (err, data) {
                  debug(err, data)
                  let progress = 0
                  let shards = 0
                  for (var i in data) {
                    const task = data[i]

                    if (task.type === 'indexer' && task.design_document === ddNewName) {
                      shards++
                      progress = progress + parseInt(task.progress, 10)
                    }
                  }

                  const overallProgress = Math.floor(progress / shards)
                  console.log('## indexing progress:', overallProgress, '%')
                  callback(null, null)
                })
              } else {
                callback(null, null)
              }
            })
          }, 3000)
        },
        function () { return !hasData },
        function (err) {
          callback(err, null)
        }
      )
    },

    // copy _NEW to live
    function (callback) {
      console.log('## copy _NEW to live', ddNewName, ddName)
      copydoc(ddNewName, ddName, function (err, data) {
        debug(err, data)
        callback(err, data)
      })
    },

    // delete the _OLD view
    function (callback) {
      console.log('## delete the _OLD view', ddOldName)
      deletedoc(ddOldName, callback)
    },

    // delete the _NEW view
    function (callback) {
      console.log('## delete the _NEW view', ddNewName)
      deletedoc(ddNewName, callback)
    }

  ], function (err, data) {
    if (err) {
      console.log(err)
    }
    console.log('FINISHED!!!')
  })
}

// load the design document
const ddFilename = argv.designdoc
if (/\.js$/.test(ddFilename)) {
  // use require to load js design doc
  const path = require('path')
  const dataAbs = path.join(process.cwd(), ddFilename.replace(/([^.]+)\.js$/, '$1'))

  migrate(null, JSON.stringify(require(dataAbs)))
} else {
  // read json
  fs.readFile(ddFilename, { encoding: 'utf8' }, migrate)
}
