'use strict'

const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const ObjectId = mongodb.ObjectId

const MongoEmitter = require('./')
const { test } = require('tape')
const abstractTests = require('mqemitter/abstractTest.js')
const dbname = 'mqemitter-test'
const collectionName = 'pubsub'
const url = 'mongodb://127.0.0.1/' + dbname

function newId () {
  return ObjectId.createFromTime(Date.now())
}
async function clean (db, cb) {
  const collections = await db.listCollections({ name: collectionName }).toArray()
  if (collections.length > 0) {
    await db.collection(collectionName).drop()
  }
  if (cb) {
    cb()
  }
}

function connectClient (url, opts, cb) {
  MongoClient.connect(url, opts)
    .then(client => {
      cb(null, client)
    })
    .catch(err => {
      cb(err)
    })
}

connectClient(url, { w: 1 }, function (err, client) {
  const db = client.db(dbname)
  if (err) {
    throw (err)
  }

  clean(db, function () {
    client.close()

    abstractTests({
      builder: function (opts) {
        opts = opts || {}
        opts.url = url

        return MongoEmitter(opts)
      },
      test
    })

    test('with default database name', function (t) {
      t.plan(2)

      const mqEmitterMongoDB = MongoEmitter({
        url
      })

      mqEmitterMongoDB.status.once('stream', function () {
        t.equal(mqEmitterMongoDB._db.databaseName, dbname)
        t.ok(true, 'database name is default db name')
        t.end()
        mqEmitterMongoDB.close()
      })
    })

    test('should fetch last packet id', function (t) {
      t.plan(3)

      let started = 0
      const lastId = newId()

      function startInstance (cb) {
        const mqEmitterMongoDB = MongoEmitter({
          url
        })

        mqEmitterMongoDB.status.once('stream', function () {
          t.equal(mqEmitterMongoDB._db.databaseName, dbname, 'database name is default db name')
          if (++started === 1) {
            mqEmitterMongoDB.emit({ topic: 'last/packet', payload: 'I\'m the last' }, () => {
              mqEmitterMongoDB.close(cb)
            })
          } else {
            t.ok(mqEmitterMongoDB._lastObj._stringId > lastId.toString(), 'Should fetch last Id')
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

      const mqEmitterMongoDB = MongoEmitter({
        url,
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

      const mqEmitterMongoDB = MongoEmitter({
        url,
        mongo: {
          appName: 'mqemitter-mongodb-test'
        }
      })

      mqEmitterMongoDB.status.once('stream', function () {
        t.equal(mqEmitterMongoDB._db.client.options.appName, 'mqemitter-mongodb-test')
        t.ok(true, 'database name is default db name')
        t.end()
        mqEmitterMongoDB.close()
      })
    })

    // keep this test as last
    test('doesn\'t throw db errors', function (t) {
      t.plan(1)

      client.close(true)

      const mqEmitterMongoDB = MongoEmitter({
        url,
        db
      })

      mqEmitterMongoDB.status.on('error', function (err) {
        t.ok(true, 'error event emitted')
        if (err.message !== 'Client must be connected before running operations') {
          t.fail('throws error')
        }
        mqEmitterMongoDB.close()
      })
    })
  })
})
