'use strict'

var mongo = require('mongojs')
var inherits = require('inherits')
var MQEmitter = require('mqemitter')
var through = require('through2')
var pump = require('pump')
var nextTick = process.nextTick
var EE = require('events').EventEmitter

function MQEmitterMongoDB (opts) {
  if (!(this instanceof MQEmitterMongoDB)) {
    return new MQEmitterMongoDB(opts)
  }

  opts = opts || {}
  opts.size = opts.size || 10 * 1024 * 1024 // 10 MB
  opts.max = opts.max || 10000 // documents
  opts.collection = opts.collection || 'pubsub'

  var url = opts.url || 'mongodb://127.0.0.1/mqemitter?auto_reconnect=true'

  this._opts = opts

  var that = this

  this._db = mongo(url, [
    opts.collection
  ])
  this._collection = this._db[opts.collection]
  this._started = false
  this.status = new EE()

  function waitStartup () {
    that._db.runCommand({ ping: 1 }, function (err, res) {
      if (that.closed) { return }

      if (err || !res.ok) {
        return setTimeout(waitStartup, 1000)
      }

      that._collection.isCapped(function (err, capped) {
        if (that.closed) { return }

        if (err) {
          // if it errs here, the collection might not exist
          that._db.createCollection(opts.collection, {
            capped: true,
            size: opts.size,
            max: opts.max
          }, start)
        } else if (!capped) {
          // the collection is not capped, make it so
          that._collection.runCommand('convertToCapped', {
            size: opts.size,
            max: opts.max
          }, start)
        } else {
          start()
        }
      })
    })
  }

  waitStartup()

  var oldEmit = MQEmitter.prototype.emit

  this._waiting = {}

  this._lastId = new mongo.ObjectId()

  var failures = 0

  function start () {
    if (that.closed) {
      return
    }

    that._stream = that._collection.find({
      _id: { $gt: that._lastId }
    }, {}, {
      tailable: true,
      timeout: false,
      awaitData: true,
      numberOfRetries: -1
    })

    that.status.emit('stream')

    pump(that._stream, through.obj(process), function () {
      if (that._started && ++failures === 10) {
        throw new Error('Lost connection to MongoDB')
      }
      setTimeout(start, 100)
    })

    function process (obj, enc, cb) {
      if (that.closed) {
        return cb()
      }

      that._started = true
      failures = 0
      that._lastId = obj._id
      oldEmit.call(that, obj, cb)

      var id = obj._id.toString()
      if (that._waiting[id]) {
        nextTick(that._waiting[id])
        delete that._waiting[id]
      }
    }
  }

  MQEmitter.call(this, opts)
}

inherits(MQEmitterMongoDB, MQEmitter)

MQEmitterMongoDB.prototype.emit = function (obj, cb) {
  var that = this
  var err

  if (this.closed) {
    err = new Error('MQEmitterMongoDB is closed')
    if (cb) {
      cb(err)
    } else {
      throw err
    }
  } else if (!this._stream) {
    // actively poll if stream is available
    this.status.once('stream', this.emit.bind(this, obj, cb))
    return this
  } else {
    this._collection.insert(obj, function (err, obj) {
      if (cb) {
        if (err) {
          cb(err)
          return
        }

        var id = obj._id.toString()
        var lastId = that._lastId.toString()
        if (id > lastId) {
          that._waiting[id] = cb
        } else {
          cb()
        }
      }
    })
  }
  return this
}

MQEmitterMongoDB.prototype.close = function (cb) {
  if (this.closed) {
    return
  }

  this.closed = true

  if (this._stream) {
    this._stream.destroy()
    this._stream.on('error', function () {})
    this._stream = null
  }

  var that = this
  MQEmitter.prototype.close.call(this, function () {
    if (that._opts.db) {
      cb()
    } else {
      // force close
      that._db.close(true, cb)
    }
  })

  return this
}

module.exports = MQEmitterMongoDB
