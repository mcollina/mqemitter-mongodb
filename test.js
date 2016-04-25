'use strict'

var mongoEmitter = require('./')
var test = require('tape').test
var abstractTests = require('mqemitter/abstractTest.js')
var mongodb = require('mongodb')

abstractTests({
  builder: function (opts) {
    opts = opts || {}
    opts.url = 'mongodb://127.0.0.1/mqemitter-test'

    return mongoEmitter(opts)
  },
  test: test
})

test('reusing a connection', function (t) {
  mongodb.connect('mongodb://127.0.0.1/mqemitter-test', function (err, db) {
    t.error(err)

    abstractTests({
      builder: function (opts) {
        opts = opts || {}
        opts.db = db

        return mongoEmitter(opts)
      },
      test: t.test.bind(t)
    })
    test.onFinish(function () {
      db.close()
    })
  })
})
