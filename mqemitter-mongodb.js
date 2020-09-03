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

  this._started = false

  function waitStartup () {
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

  that._oldEmit = MQEmitter.prototype.emit

  this._waiting = new Map()
  this._queue = [];

  var failures = 0

  function setLast () {
    that._collection
    .find({}, { timeout: false })
    .sort({ $natural : -1 })
    .limit(1)
    .next(function (err, doc) {
      if(err){
        that.status.emit('error', err)
      }

      that._lastObj = doc ? doc : { _id: new mongodb.ObjectID() };

      if(!that._lastObj._stringId) {
        that._lastObj._stringId = that._lastObj._id.toString()
      }

      start()
    });  
  }

  function start () {

    that._stream = that._collection.find({ _id: { $gt: that._lastObj._id }}, {
      tailable: true,
      timeout: false,
      awaitData: true,
      numberOfRetries: -1
    })

    pump(that._stream, through.obj(process), function () {
      if (that.closed) {
        return
      }

      if (that._started && ++failures === 10) {
        that.status.emit('error', new Error('Lost connection to MongoDB'))
      }
      setTimeout(start, 100)
    })

    that.status.emit('stream')

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
      const index = that._findNext(obj._stringId)
      if(index >= 0) {
        that._queue[index]._done = true
        that._checkDone()
      } else {
        // in a cluster env we will not have all packets in our queue so simply emit it
        that._emitPacket(obj)
      }

      // process next
      cb()
    }
  }

  MQEmitter.call(this, opts)
}

inherits(MQEmitterMongoDB, MQEmitter)

// returns the index of the packet in _queue
MQEmitterMongoDB.prototype._findNext = function(id) {
  for (var i = 0, len = this._queue.length; i < len; i++) {
    if(this._queue[i]._stringId === id) {
      return i
    }    
  }

  return -1
}

// emits a packet if specified or the first packet in the queue
MQEmitterMongoDB.prototype._emitPacket = function(obj) {
  var obj = obj || this._queue.shift()
  // updates lastId
  this._lastObj = obj
  // once done check if there are other packets to emit
  this._oldEmit.call(this, obj, this._checkDone.bind(this))
  // checks for waiting callbacks
  if (this._waiting.has(obj._stringId)) {
    nextTick(this._waiting.get(obj._stringId))
    this._waiting.delete(obj._stringId)
  }
}

// checks if the first packet in the queue is processed (done) if so it emit it
MQEmitterMongoDB.prototype._checkDone = function() {
  if(this._queue[0] && this._queue[0]._done) {
    this._emitPacket()
  }
}

MQEmitterMongoDB.prototype.emit = function (obj, cb) {
  var that = this
  var err

  if (!this.closed && !this._stream) {
    // actively poll if stream is available
    this.status.once('stream', this.emit.bind(this, obj, cb))
    return this
  } else if (this.closed) {
    err = new Error('MQEmitterMongoDB is closed')
    if (cb) {
      cb(err)
    }
  } else {
    const id = new mongodb.ObjectID(obj._id)
    obj._id = id
    obj._stringId = id.toString()
    that._queue.push(obj)

    this._collection.insertOne(obj, function (err, res) {
      if (cb) {
        if (err) {
          cb(err)
          return
        }

        var lastObj = that._lastObj
        var t1 = id.getTimestamp().getTime()
        var t2 = lastObj._id.getTimestamp().getTime()

        // we need to check only the date part
        if (t1 < t2) {
          cb()
          return
        } else if (t1 === t2) {
          // we need to dig deeper and check the ObjectId counter
          var b1 = Buffer.from(obj._stringId, 'hex')
          var b2 = Buffer.from(lastObj._stringId, 'hex')

          // if they are not equals we call the callback
          // immediately to avoid leaks
          if (!b1.slice(4, 8).equals(b2.slice(4, 8))) {
            cb()
            return
          }

          // the last three bytes are the random counter
          var one = (b1[9] << 16) + (b1[10] << 8) + b1[11]
          var two = (b2[9] << 16) + (b2[10] << 8) + b2[11]

          // for some reasons we have to increase two by one
          // or we will leak data
          if (one <= two + 1) {
            // TODO investigate we we need to increment by 1 and delay by 50ms
            // to not leak
            setTimeout(cb, 50)
            return
          }
        }

        that._waiting.set(obj._stringId, cb)
      }
    })
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
  this._stream.on('error', function () {})
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

function noop () {}

module.exports = MQEmitterMongoDB
