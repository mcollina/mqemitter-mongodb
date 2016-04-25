'use strict'

var mongo = require('mongojs')
var inherits = require('inherits')
var MQEmitter = require('mqemitter')
var through = require('through2')
var pump = require('pump')

function MQEmitterMongoDB (opts) {
  if (!(this instanceof MQEmitterMongoDB)) {
    return new MQEmitterMongoDB(opts)
  }

  opts = opts || {}
  opts.size = opts.size || 10 * 1024 * 1024 // 10 MB
  opts.max = opts.max || 10000 // documents
  opts.collection = opts.collection || 'pubsub'

  var conn = opts.db || opts.url || 'mongodb://127.0.0.1/mqemitter?auto_reconnect=true'

  this._opts = opts

  var that = this

  this._db = mongo(conn)
  this._collection = this._db.collection(opts.collection)
  this._started = false

  function waitStartup () {
    that._db.runCommand({ ping: 1 }, function (err, res) {
      if (that.closed) { return }

      if (err || !res.ok) {
        return setTimeout(waitStartup, 1000)
      }

      that._collection.isCapped(function (err, capped) {
        if (that.closed) { return }

        // if it errs here, the collection might not be there
        if (err || !capped) {
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

  this._tounlock = []

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
      setImmediate(unlock)
    }
  }

  MQEmitter.call(this, opts)

  function unlock () {
    var tounlock = that._tounlock
    for (var i = 0; i < tounlock.length; i++) {
      tounlock[i]()
    }
    that._tounlock = []
  }
}

inherits(MQEmitterMongoDB, MQEmitter)

MQEmitterMongoDB.prototype.emit = function (obj, cb) {
  var err
  var tounlock = this._tounlock
  if (this.closed) {
    err = new Error('MQEmitterMongoDB is closed')
    if (cb) {
      cb(err)
    } else {
      throw err
    }
  } else {
    if (cb) {
      tounlock.push(cb)
    }

    this._collection.insert(obj, function (err) {
      var i
      if (err) {
        if (cb) {
          i = tounlock.indexOf(cb)
          tounlock.splice(i, 1)
          return cb(err)
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
