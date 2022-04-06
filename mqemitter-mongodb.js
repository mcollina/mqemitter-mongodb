'use strict'

const urlModule = require('url')
const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const inherits = require('inherits')
const MQEmitter = require('mqemitter')
const through = require('through2')
const pump = require('pump')
const nextTick = process.nextTick
const EE = require('events').EventEmitter

function toStream (op) {
  return op.stream ? op.stream() : op
}

function MQEmitterMongoDB (opts) {
  if (!(this instanceof MQEmitterMongoDB)) {
    return new MQEmitterMongoDB(opts)
  }

  opts = opts || {}
  opts.size = opts.size || 10 * 1024 * 1024 // 10 MB
  opts.max = opts.max || 10000 // documents
  opts.collection = opts.collection || 'pubsub'

  var url = opts.url || 'mongodb://127.0.0.1/mqemitter'
  this.status = new EE()
  this.status.setMaxListeners(0)

  this._opts = opts

  var that = this

  this._db = null

  if (opts.db) {
    that._db = opts.db
    setImmediate(waitStartup)
  } else {
    var defaultOpts = { useNewUrlParser: true, useUnifiedTopology: true }
    var mongoOpts = that._opts.mongo ? Object.assign(defaultOpts, that._opts.mongo) : defaultOpts
    MongoClient.connect(url, mongoOpts, function (err, client) {
      if (err) {
        return that.status.emit('error', err)
      }
      /* eslint-disable */
      var urlParsed = urlModule.parse(that._opts.url)
      var databaseName = that._opts.database || (urlParsed.pathname ? urlParsed.pathname.substr(1) : undefined)
      databaseName = databaseName.substr(databaseName.lastIndexOf('/') + 1)

      that._client = client
      that._db = client.db(databaseName)

      waitStartup()
    })
  }

  this._hasStream = false
  this._started = false

  function waitStartup() {
    that._collection = that._db.collection(opts.collection)
    
      that._collection.isCapped(function (err, capped) {
        if (that.closed) { return }

        if (err) {
          // if it errs here, the collection might not exist
          that._db.createCollection(opts.collection, {
            capped: true,
            size: opts.size,
            max: opts.max
          }, setLast)
        } else if (!capped) {
          // the collection is not capped, make it so
          that._db.command({
            convertToCapped: opts.collection,
            size: opts.size,
            max: opts.max
          }, setLast)
        } else {
          setLast()
        }
      })
  }

  const oldEmit = MQEmitter.prototype.emit

  this._waiting = new Map()
  this._queue = []
  this._executingBulk = false
  var failures = 0

  function setLast() {    
    try {
    that._collection
      .find({}, { timeout: false })
      .sort({ $natural: -1 })
      .limit(1)
      .next(function (err, doc) {
        if (err) {
          that.status.emit('error', err)
        }

        that._lastObj = doc ? doc : { _id: new mongodb.ObjectID() };

        if (!that._lastObj._stringId) {
          that._lastObj._stringId = that._lastObj._id.toString()
        }

        start()
      });
    } catch (error) {
      that.status.emit('error', error)
    }
  }

  function start() {
    if (that.closed) { return }
    that._stream = toStream(that._collection.find({ _id: { $gt: that._lastObj._id } }, {
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

    function process(obj, enc, cb) {
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

MQEmitterMongoDB.prototype._bulkInsert = function () {
  const that = this
  if (!this._executingBulk && this._queue.length > 0) {
    this._executingBulk = true
    var bulk = this._collection.initializeOrderedBulkOp()

    while (this._queue.length) {
      var p = this._queue.shift()
      bulk.insert(p.obj)
    }

    bulk.execute(function (err) {
      that._executingBulk = false
      that._bulkInsert()
    })
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
    var err = new Error('MQEmitterMongoDB is closed')
    if (cb) {
      cb(err)
    }
  } else {
    obj._id = new mongodb.ObjectID(obj._id)
    obj._stringId = obj._id.toString()
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

  var that = this
  MQEmitter.prototype.close.call(this, function () {
    if (that._opts.db) {
      cb()
    } else {
      that._client.close(cb)
    }
  })

  return this
}

function noop() { }

module.exports = MQEmitterMongoDB
