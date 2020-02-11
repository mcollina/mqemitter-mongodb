'use strict'

var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var MongoEmitter = require('./')
var test = require('tape').test
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
      t.plan(3)

      var mqEmitterMongoDB = MongoEmitter({
        url: url,
        mongo: {
          keepAlive: false,
          poolSize: 9
        }
      })

      mqEmitterMongoDB.status.once('stream', function () {
        t.equal(mqEmitterMongoDB._db.topology.s.options.keepAlive, false)
        t.equal(mqEmitterMongoDB._db.topology.s.options.poolSize, 9)
        t.ok(true, 'database name is default db name')
        t.end()
        mqEmitterMongoDB.close()
      })
    })
  })
})
