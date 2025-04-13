'use strict'

const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const ObjectId = mongodb.ObjectId

const MongoEmitter = require('./')
const { test } = require('node:test')
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
    process.nextTick(cb)
  }
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

    test('with default database name', async function (t) {
      t.plan(2)

      await new Promise((resolve) => {
        const mqEmitterMongoDB = MongoEmitter({
          url
        })

        mqEmitterMongoDB.status.once('stream', async function () {
          t.assert.equal(mqEmitterMongoDB._db.databaseName, dbname)
          t.assert.ok(true, 'database name is default db name')
          mqEmitterMongoDB.close()
          resolve()
        })
      })
    })

    test('should fetch last packet id', async function (t) {
      t.plan(3)

      let started = 0
      const lastId = newId()

      function startInstance (cb) {
        const mqEmitterMongoDB = MongoEmitter({
          url
        })

        mqEmitterMongoDB.status.once('stream', function () {
          t.assert.equal(mqEmitterMongoDB._db.databaseName, dbname, 'database name is default db name')
          if (++started === 1) {
            mqEmitterMongoDB.emit({ topic: 'last/packet', payload: 'I\'m the last' }, () => {
              mqEmitterMongoDB.close(cb)
            })
          } else {
            t.assert.ok(mqEmitterMongoDB._lastObj._stringId > lastId.toString(), 'Should fetch last Id')
            mqEmitterMongoDB.close(cb)
          }
        })
      }

      await new Promise((resolve) => {
        startInstance(function () {
          startInstance(resolve)
        })
      })
    })

    test('with database option', async function (t) {
      t.plan(2)

      await new Promise((resolve) => {
        const mqEmitterMongoDB = MongoEmitter({
          url,
          database: 'test-custom-db-name'
        })

        mqEmitterMongoDB.status.once('stream', function () {
          t.assert.equal(mqEmitterMongoDB._db.databaseName, 'test-custom-db-name')
          t.assert.ok(true, 'database name is custom db name')
          mqEmitterMongoDB.close()
          resolve()
        })
      })
    })

    test('with mongodb options', async function (t) {
      t.plan(2)

      await new Promise((resolve) => {
        const mqEmitterMongoDB = MongoEmitter({
          url,
          mongo: {
            appName: 'mqemitter-mongodb-test'
          }
        })

        mqEmitterMongoDB.status.once('stream', function () {
          t.assert.equal(mqEmitterMongoDB._db.client.options.appName, 'mqemitter-mongodb-test')
          t.assert.ok(true, 'database name is default db name')
          mqEmitterMongoDB.close()
          resolve()
        })
      })
    })

    // keep this test as last
    test('doesn\'t throw db errors', async function (t) {
      t.plan(1)

      await new Promise((resolve) => {
        client.close(true)

        const mqEmitterMongoDB = MongoEmitter({
          url,
          db
        })

        mqEmitterMongoDB.status.on('error', function (err) {
          t.assert.ok(true, 'error event emitted')
          if (err.message !== 'Client must be connected before running operations') {
            t.assert.fail('throws error')
          }
          mqEmitterMongoDB.close()
          resolve()
        })
      })
    })
  })
})
