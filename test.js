'use strict'

var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var MongoEmitter = require('./')
var { test } = require('tape')
var abstractTests = require('mqemitter/abstractTest.js')
var clean = require('mongo-clean')
var dbname = 'mqemitter-test'
var url = 'mongodb://127.0.0.1/' + dbname

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true, w: 1 }, function (err, client) {
  if (err) {
    throw err
  }

  var db = client.db(dbname)

  clean(db, function (err) {
    if (err) {
      throw err
    }

    client.close()

    abstractTests({
      builder: function (opts) {
        opts = opts || {}
        opts.url = url

        return MongoEmitter(opts)
      },
      test: test
    })

    test('with default database name', function (t) {
      t.plan(2)

      var mqEmitterMongoDB = MongoEmitter({
        url: url
      })

      mqEmitterMongoDB.status.once('stream', function () {
        t.equal(mqEmitterMongoDB._db.databaseName, dbname)
        t.ok(true, 'database name is default db name')
        t.end()
        mqEmitterMongoDB.close()
      })
    })

    test('should fetch last packet id', function (t) {
      t.plan(5)

      var started = 0
      var lastId = new mongodb.ObjectId()

      function startInstance (cb) {
        var mqEmitterMongoDB = MongoEmitter({
          url: url
        })

        mqEmitterMongoDB.status.once('stream', function () {
          t.equal(mqEmitterMongoDB._db.databaseName, dbname)
          t.ok(true, 'database name is default db name')
          if (++started === 1) {
            mqEmitterMongoDB.emit({ topic: 'last/packet', payload: 'I\'m the last', _id: lastId }, mqEmitterMongoDB.close.bind(mqEmitterMongoDB, cb))
          } else {
            t.equal(mqEmitterMongoDB._lastObj._stringId, lastId.toString(), 'Should fetch last Id')
            mqEmitterMongoDB.close(cb)
          }
        })
      }

      startInstance(function () {
        startInstance(t.end.bind(t))
      })
    })

    test('with database option', function (t) {
      t.plan(2)

      var mqEmitterMongoDB = MongoEmitter({
        url: url,
        database: 'test-custom-db-name'
      })

      mqEmitterMongoDB.status.once('stream', function () {
        t.equal(mqEmitterMongoDB._db.databaseName, 'test-custom-db-name')
        t.ok(true, 'database name is custom db name')
        t.end()
        mqEmitterMongoDB.close()
      })
    })

    test('with mongodb options', function (t) {
      t.plan(2)

      var mqEmitterMongoDB = MongoEmitter({
        url: url,
        mongo: {
          keepAlive: false
        }
      })

      mqEmitterMongoDB.status.once('stream', function () {
        t.equal(mqEmitterMongoDB._db.s.client.s.options.keepAlive, false)
        t.ok(true, 'database name is default db name')
        t.end()
        mqEmitterMongoDB.close()
      })
    })

    // keep this test as last
    test('doesn\'t throw db errors', function (t) {
      t.plan(1)

      client.close(true)

      var mqEmitterMongoDB = MongoEmitter({
        url: url,
        db: db
      })

      mqEmitterMongoDB.status.on('error', function (err) {
        t.ok(true, 'error event emitted')
        if (err.message !== 'MongoClient must be connected to perform this operation') {
          t.fail('throws error')
        }
        mqEmitterMongoDB.close()
      })
    })
  })
})
