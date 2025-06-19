import fs from 'node:fs'
import path from 'node:path'
import { parseArgs } from 'node:util'
import * as ccurllib from 'ccurllib'

const app = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, 'package.json'), { encoding: 'utf8' }))
const syntax =
`Syntax:
--url/-u           (COUCH_URL)      CouchDB URL              (required)
--database/--db    (COUCH_DATABASE) CouchDB Datbase name     (required)
--designdoc/--dd                    Design document filename (required)
`
const URL = process.env.COUCH_URL
const DB = process.env.COUCH_DATABASE

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

const getReqObj = (method) => {
  const obj = {
    method: method || 'get',
    url: `${values.url}/${values.database}`,
    headers: {
      'content-type': 'application/json',
      'User-Agent': `${app.name}/${app.version}`
    }
  }
  return obj
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

const debug = (status, data) => {
  console.log('  status = ', status)
  console.log('  data = ', JSON.stringify(data))
  console.log('-------------------------------')
}

const copydoc = async (fromId, toId) => {
  let fromDoc = null
  let toDoc = null
  let req, res

  // fetch the document we are copying
  console.log('## copydoc - Fetching from', fromId)
  req = getReqObj()
  req.url += `/${fromId}`
  res = await ccurllib.request(req) //await db.get(fromId)
  if (res.status >= 400){
    console.log(`docId ${fromId} does not exist - nothing to do`)
    return
  }
  fromDoc = res.result

  // fetch the document we are copying to (if it is there)
  req = getReqObj()
  req.url += `/${toId}`
  res = await ccurllib.request(req) // await db.get(toId)
  if (res.status < 300) {
    toDoc = res.result
  }

  // overwrite the destination
  console.log('## copydoc - Writing new to', toId)
  fromDoc._id = toId
  if (toDoc) {
    fromDoc._rev = toDoc._rev
  } else {
    delete fromDoc._rev
  }
  req = getReqObj()
  req.data = fromDoc
  req.method = 'post'
  res = await ccurllib.request(req) // await db.insert(fromDoc)
  if (res.status >= 300) {
    throw new Error(`Could not copy from ${fromId} to ${toId} - HTTP ${res.status} ${res.result}`)
  }
}

const writedoc = async function (obj, docid) {
  let preexistingdoc = null
  let req, res

  console.log('## writedoc - Looking for pre-existing', docid)
  req = getReqObj()
  req.url += `/${docid}`
  res = await ccurllib.request(req) // await db.get(docid)
  if (res.status < 300) {
    preexistingdoc = res.result
  }

  obj._id = docid
  if (preexistingdoc) {
    obj._rev = preexistingdoc._rev
  }
  console.log('## writedoc - Writing doc', obj)
  req = getReqObj()
  req.data = obj
  req.method = 'post'
  res = await ccurllib.request(req) // data = await db.insert(obj)
  debug(res.status, res.result)
  if (res.status >= 300) {
    throw new Error(`Could not write docId from ${docid} - HTTP ${res.status} ${res.result}`)
  }
  
}

const deletedoc = async function (docid) {
  let req, res
  console.log('## deletedoc - Looking for docid', docid)
  req = getReqObj()
  req.url += `/${docid}`
  res = await ccurllib.request(req) // await db.get(docid)
  debug(res.status, res.result)
  if (res.status >= 400) {
    debug(null, `Document ${docid} does not exist`)
    return
  }

  console.log('## deletedoc - Deleting ', docid, res.result._rev)
  req = getReqObj()
  req.url += `/${docid}`
  req.qs = {}
  req.qs.rev = res.result._rev
  req.method = 'delete'
  res = await ccurllib.request(req) // data = await db.insert(obj)
  debug(res.status, res.result)
  if (res.status >= 300) {
    throw new Error(`Could not delete docId ${docId} - HTTP ${res.status} ${res.result}`)
  }
  // await db.destroy(docid, data._rev)
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
  let dd, data, req, res
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
  console.log('## check db exists')
  req = getReqObj()
  data = await ccurllib.request(req) // await nano.db.get(values.database)
  debug(data.status, data.result)
  if (data.status >= 400) {
    console.error('databases does not exist')
    process.exit(1)
  }

  // check that the existing view isn't the same as the incoming view
  console.log('## check existing view is not the same as the incoming view')
  req = getReqObj()
  req.url += `/${ddName}`
  data = await ccurllib.request(req)
  if (data.result > 300) {
    res = ''
  } else {
    res = data.result
  }
  // data = await db.get(ddName)
  const a = clone(res)
  const b = clone(dd)
  delete a._rev
  delete a._id
  delete b._rev
  delete b._id
  if (JSON.stringify(a) === JSON.stringify(b)) {
    console.error('** The design document is the same, no need to migrate! **')
    process.exit(2)
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
    hasData = false
    if (typeof dd.views === 'object' && Object.keys(dd.views).length > 0) {
      const path = `${ddNewName}/_view/${Object.keys(dd.views)[0]}`
      await sleep(3000)
      console.log('## query ',path, 'to validate freshness.')
      req = getReqObj()
      req.url += '/' + path
      req.qs = { limit: 1}
      res = await ccurllib.request(req)
      if (res.status < 300) {
        hasData = true
      }
    }
    if (!hasData) {
      // get progress from active tasks
      req = getReqObj()
      req.url = `${values.url}/_active_tasks`
      res = await ccurllib.request(req)
      data = res.result
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

export default async function () {
  // load the design document
  const ddFilename = values.designdoc

  if (/\.js$/.test(ddFilename)) {
    // use require to load js design doc
    const dataAbs = path.join(process.cwd(), ddFilename)
    const dd = await import(dataAbs)
    await migrate(JSON.stringify(dd.default))
  } else {
    // read json
    const str = fs.readFileSync(ddFilename, { encoding: 'utf8' })
    await migrate(str)
  }
}
