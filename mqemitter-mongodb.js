
var mongo     = require('mongojs')
  , inherits  = require('inherits')
  , MQEmitter = require('mqemitter')
  , through   = require('through2')
  , eos       = require('end-of-stream')

function MQEmitterMongoDB(opts) {
  if (!(this instanceof MQEmitterMongoDB)) {
    return new MQEmitterMongoDB(opts)
  }

  opts = opts || {}
  opts.size = opts.size || 10 * 1024 * 1024, // 10 MB
  opts.max = opts.max || 10000 // documents
  opts.collection = opts.collection || 'pubsub'
  opts.url = opts.url || 'mongodb://127.0.0.1/mqemitter?auto_reconnect'

  this._opts = opts

  var that = this

  this._db = mongo(opts.url)
  this._collection = this._db.collection(opts.collection)

  this._collection.isCapped(function(err, capped) {
    if (that.closed) { return }
    if (err) { throw err } // we just don't care, die horribly

    if (!capped) {
      // the collection is not capped, make it so
      that._collection.runCommand('convertToCapped', {
        size: opts.size,
        max: opts.max
      }, start)
    } else {
      start()
    }
  })

  var oldEmit = MQEmitter.prototype.emit

  this._lastId = new mongo.ObjectId()

  var failures = 0
  function start() {
    if (that.closed) {
      return
    }

    if (failures++ === 10) {
      throw new Error('Connection to mongo is dead')
    }

    that._stream = that._collection.find({
      _id: { $gt: that._lastId }
    }, {
        tailable: true
      , timeout: false
      , awaitdata: true
      , numberOfRetries: -1
    })

    eos(that._stream, start)

    that._stream.pipe(through.obj(function(obj, enc, cb) {
      if (that.closed) {
        return cb()
      }

      failures = 0
      that._lastId = obj._id
      oldEmit.call(that, obj, cb)
    }))
  }

  MQEmitter.call(this, opts)
}

inherits(MQEmitterMongoDB, MQEmitter)

function nop() {}
MQEmitterMongoDB.prototype.emit = function(obj, cb) {
  this._collection.insert(obj, { w: 1 }, cb || nop)
  return this
}

MQEmitterMongoDB.prototype.close = function(cb) {
  if (this.closed) {
    return
  }

  if (this._stream) {
    this._stream.destroy()
    this._stream.on('error', function() {})
    this._stream = null
  }

  var that = this
  MQEmitter.prototype.close.call(this, function() {
    that._db.close(cb)
  })

  return this
}

module.exports = MQEmitterMongoDB
