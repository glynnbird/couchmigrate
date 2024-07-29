const fs = require('fs')
const syntax =
`Syntax:
--url/-u           (COUCH_URL)      CouchDB URL              (required)
--database/--db    (COUCH_DATABASE) CouchDB Datbase name     (required)
--designdoc/--dd                    Design document filename (required)
`
const URL = process.env.COUCH_URL
const DB = process.env.COUCH_DATABASE
const { parseArgs } = require('node:util')
const argv = process.argv.slice(2)
const options = {
  url: {
    type: 'string',
    short: 'u',
    default: URL
  },
  database: {
    type: 'string',
    short: 'd',
    default: DB
  },
  db: {
    type: 'string',
    default: DB
  },
  designdoc: {
    type: 'string'
  },
  dd: {
    type: 'string'
  },
  help: {
    type: 'boolean',
    short: 'h',
    default: false
  }
}

// parse command-line options
const { values } = parseArgs({ argv, options })
if (values.db) {
  values.database = values.db
  delete values.db
}
if (values.dd) {
  values.designdoc = values.dd
  delete values.dd
}

// help mode
if (values.help) {
  console.log(syntax)
  process.exit(0)
}

// mandatory parameters
if (!values.url || !values.designdoc || !values.database) {
  console.log('You must supply a URL, database and design doc')
  process.exit(1)
}

// get COUCH_URL from the environment
let nano = null
let db = null

const debug = (err, data) => {
  console.log('  err = ', (err) ? 'true' : '')
  console.log('  data = ', JSON.stringify(data))
  console.log('-------------------------------')
}

const copydoc = async (fromId, toId) => {
  let fromDoc = null
  let toDoc = null

  // fetch the document we are copying
  console.log('## copydoc - Fetching from', fromId)
  try {
    fromDoc = await db.get(fromId)
  } catch {
    console.log(`docId ${fromId} does not exist - nothing to do`)
    return
  }

  // fetch the document we are copying to (if it is there)
  try {
    toDoc = await db.get(toId)
  } catch {
    // target not there
  }

  // overwrite the destination
  console.log('## copydoc - Writing new to', toId)
  fromDoc._id = toId
  if (toDoc) {
    fromDoc._rev = toDoc._rev
  } else {
    delete fromDoc._rev
  }
  await db.insert(fromDoc)
}

const writedoc = async function (obj, docid) {
  let preexistingdoc = null
  let data

  console.log('## writedoc - Looking for pre-existing', docid)
  try {
    data = await db.get(docid)
    preexistingdoc = data
  } catch {
    // doc does not exist
  }
  obj._id = docid
  if (preexistingdoc) {
    obj._rev = preexistingdoc._rev
  }
  console.log('## writedoc - Writing doc', obj)
  data = await db.insert(obj)
  debug(null, data)
}

const deletedoc = async function (docid) {
  let data
  console.log('## deletedoc - Looking for docid', docid)
  try {
    data = await db.get(docid)
  } catch {
    return
  }
  debug(null, data)
  console.log('## deletedoc - Deleting ', docid, data._rev)
  await db.destroy(docid, data._rev)
}

const clone = function (x) {
  return JSON.parse(JSON.stringify(x))
}

// promisey-sleep
const sleep = async (ms) => {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, ms)
  })
}

const migrate = async function (ddDocString) {
  // this is the whole design document
  let dd, data
  try {
    dd = JSON.parse(ddDocString)
  } catch (e) {
    console.log('FAILED to parse file contents as JSON - cannot continue')
    process.exit(1)
  }

  const ddName = dd._id
  delete dd._rev
  const ddOldName = ddName + '_OLD'
  const ddNewName = ddName + '_NEW'

  // check that the database exists
  try {
    console.log('## check db exists')
    data = await nano.db.get(values.database)
    debug(null, data)
  } catch {
    throw new Error('database does not exist')
  }

  // check that the existing view isn't the same as the incoming view
  try {
    console.log('## check existing view is not the same as the incoming view')
    data = await db.get(ddName)
    const a = clone(data)
    const b = clone(dd)
    delete a._rev
    delete a._id
    delete b._rev
    delete b._id
    if (JSON.stringify(a) === JSON.stringify(b)) {
      console.log('** The design document is the same, no need to migrate! **')
      throw new Error('design document is the same')
    }
  } catch {
    // no pre-existing ddoc
  }

  // copy original design document to _OLD
  console.log('## copy original design document to _OLD')
  await copydoc(ddName, ddOldName)

  // write new design document to _NEW
  console.log('## write new design document to _NEW')
  await writedoc(dd, ddNewName)

  // wait for the view build to complete, by polling
  let hasData = false
  do {
    const name = dd._id.replace(/_design\//, '')
    const v = Object.keys(dd.views)[0]
    await sleep(3000)
    console.log('## query ', name, '/', v, 'to validate freshness.')
    try {
      data = await db.view(name, v, { limit: 1 })
      hasData = !!data
    } catch {
      // view timed out
    }
    if (!hasData) {
      // get progress from active tasks
      data = await nano.request({ path: '_active_tasks' })
      let progress = 0
      let shards = 0
      for (const i in data) {
        const task = data[i]

        if (task.type === 'indexer' && task.design_document === ddNewName) {
          shards++
          progress = progress + parseInt(task.progress, 10)
        }
      }
      const overallProgress = Math.floor(progress / shards)
      console.log('## indexing progress:', overallProgress, '%')
    }
  } while (!hasData)

  // copy _NEW to live
  console.log('## copy _NEW to live', ddNewName, ddName)
  await copydoc(ddNewName, ddName)

  // delete the _OLD view
  console.log('## delete the _OLD view', ddOldName)
  await deletedoc(ddOldName)

  // delete the _NEW view
  console.log('## delete the _NEW view', ddNewName)
  await deletedoc(ddNewName)
  console.log('FINISHED!!!')
}

const main = async () => {
  // load the design document
  const ddFilename = values.designdoc
  const iam = require('./iam.js')
  const t = await iam.getToken(process.env.IAM_API_KEY)
  const opts = {
    url: values.url,
    requestDefaults: {
      timeout: 10000,
      headers: {
        'User-Agent': 'couchmigrate',
        'x-cloudant-io-priority': 'low'
      }
    }
  }
  if (t) {
    opts.defaultHeaders = { Authorization: 'Bearer ' + t }
  }
  nano = require('nano')(opts)
  db = nano.db.use(values.database)

  if (/\.js$/.test(ddFilename)) {
    // use require to load js design doc
    const path = require('path')
    const dataAbs = path.join(process.cwd(), ddFilename.replace(/([^.]+)\.js$/, '$1'))
    await migrate(JSON.stringify(require(dataAbs)))
  } else {
    // read json
    const str = fs.readFileSync(ddFilename, { encoding: 'utf8' })
    await migrate(str)
  }
}
main()
