'use strict'

const urlModule = require('url')
const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const ObjectId = mongodb.ObjectId
const inherits = require('inherits')
const MQEmitter = require('mqemitter')
const through = require('through2')
const pump = require('pump')
const nextTick = process.nextTick
const EE = require('events').EventEmitter

function toStream (op) {
  return op.stream ? op.stream() : op
}

function newId () {
  return ObjectId.createFromTime(Date.now())
}

function connectClient (url, opts, cb) {
  MongoClient.connect(url, opts)
    .then(client => {
      process.nextTick(cb, null, client)
    })
    .catch(err => {
      process.nextTick(cb, err)
    })
}

// check if the collection exists and is capped
// if not create it
async function checkCollection (ctx, next) {
  // ping to see if database is connected
  try {
    await ctx._db.command({ ping: 1 })
  } catch (err) {
    ctx.status.emit('error', err)
    return
  }
  const collectionName = ctx._opts.collection
  const collections = await ctx._db.listCollections({ name: collectionName }).toArray()
  if (collections.length > 0) {
    ctx._collection = ctx._db.collection(collectionName)
    if (!await ctx._collection.isCapped()) {
      // the collection is not capped, make it so
      await ctx._db.command({
        convertToCapped: collectionName,
        size: ctx._opts.size,
        max: ctx._opts.max
      })
    }
  } else {
    // collection does not exist yet create it
    await ctx._db.createCollection(collectionName, {
      capped: true,
      size: ctx._opts.size,
      max: ctx._opts.max
    })
    ctx._collection = ctx._db.collection(collectionName)
  }
  process.nextTick(next)
}

function MQEmitterMongoDB (opts) {
  if (!(this instanceof MQEmitterMongoDB)) {
    return new MQEmitterMongoDB(opts)
  }

  opts = opts || {}
  opts.size = opts.size || 10 * 1024 * 1024 // 10 MB
  opts.max = opts.max || 10000 // documents
  opts.collection = opts.collection || 'pubsub'

  const url = opts.url || 'mongodb://127.0.0.1/mqemitter'
  this.status = new EE()
  this.status.setMaxListeners(0)

  this._opts = opts

  const that = this

  this._db = null

  if (opts.db) {
    that._db = opts.db
    setImmediate(waitStartup)
  } else {
    const defaultOpts = { }
    const mongoOpts = that._opts.mongo ? Object.assign(defaultOpts, that._opts.mongo) : defaultOpts
    connectClient(url, mongoOpts, function (err, client) {
      if (err) {
        return that.status.emit('error', err)
      }
      const urlParsed = new urlModule.URL(that._opts.url)
      let databaseName = that._opts.database || (urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined)
      databaseName = databaseName.substr(databaseName.lastIndexOf('/') + 1)

      that._client = client
      that._db = client.db(databaseName)

      waitStartup()
    })
  }

  this._hasStream = false
  this._started = false

  function waitStartup () {
    checkCollection(that, setLast)
  }

  const oldEmit = MQEmitter.prototype.emit

  this._waiting = new Map()
  this._queue = []
  this._executingBulk = false
  let failures = 0

  async function setLast () {
    try {
      const results = await that._collection
        .find({}, { timeout: false })
        .sort({ $natural: -1 })
        .limit(1)
        .toArray()
      const doc = results[0]
      that._lastObj = doc || { _id: newId() }

      if (!that._lastObj._stringId) {
        that._lastObj._stringId = that._lastObj._id.toString()
      }

      await start()
    } catch (error) {
      that.status.emit('error', error)
    }
  }

  async function start () {
    if (that.closed) { return }

    that._stream = toStream(await that._collection.find({ _id: { $gt: that._lastObj._id } }, {
      tailable: true,
      timeout: false,
      awaitData: true
    }))

    pump(that._stream, through.obj(process), function () {
      if (that.closed) {
        return
      }

      if (that._started && ++failures === 10) {
        that.status.emit('error', new Error('Lost connection to MongoDB'))
      }
      setTimeout(start, 100)
    })

    that._hasStream = true
    that.status.emit('stream')
    that._bulkInsert()

    function process (obj, enc, cb) {
      if (that.closed) {
        return cb()
      }

      // convert mongo binary to buffer
      if (obj.payload && obj.payload._bsontype) {
        obj.payload = obj.payload.read(0, obj.payload.length())
      }

      that._started = true
      failures = 0
      that._lastObj = obj

      oldEmit.call(that, obj, cb)

      if (that._waiting.has(obj._stringId)) {
        nextTick(that._waiting.get(obj._stringId))
        that._waiting.delete(obj._stringId)
      }
    }
  }

  MQEmitter.call(this, opts)
}

inherits(MQEmitterMongoDB, MQEmitter)

MQEmitterMongoDB.prototype._bulkInsert = async function () {
  if (!this._executingBulk && this._queue.length > 0) {
    this._executingBulk = true
    const operations = []

    while (this._queue.length) {
      const p = this._queue.shift()
      operations.push({ insertOne: p.obj })
    }

    await this._collection.bulkWrite(operations)
    this._executingBulk = false
    this._bulkInsert()
  }
}

MQEmitterMongoDB.prototype._insertDoc = function (obj, cb) {
  if (cb) {
    this._waiting.set(obj._stringId, cb)
  }
  this._queue.push({ obj })

  if (this._hasStream) {
    this._bulkInsert()
  }
}

MQEmitterMongoDB.prototype.emit = function (obj, cb) {
  if (!this.closed && !this._stream) {
    // actively poll if stream is available
    this.status.once('stream', this.emit.bind(this, obj, cb))
    return this
  } else if (this.closed) {
    const err = new Error('MQEmitterMongoDB is closed')
    if (cb) {
      cb(err)
    }
  } else {
    this._insertDoc(obj, cb)
  }

  return this
}

MQEmitterMongoDB.prototype.close = function (cb) {
  cb = cb || noop

  if (this.closed) {
    return cb()
  }

  if (!this._stream) {
    this.status.once('stream', this.close.bind(this, cb))
    return
  }

  this._stream.destroy()
  this._stream.on('error', function () { })
  this._stream = null

  this.closed = true

  const that = this
  MQEmitter.prototype.close.call(this, async function () {
    if (that._opts.db) {
      cb()
    } else {
      await that._client.close()
      process.nextTick(cb)
    }
  })

  return this
}

function noop () { }

module.exports = MQEmitterMongoDB
