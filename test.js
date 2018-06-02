'use strict'

var mongodb = require('mongodb')
var MongoClient = mongodb.MongoClient
var MongoEmitter = require('./')
var test = require('tape').test
var abstractTests = require('mqemitter/abstractTest.js')
var clean = require('mongo-clean')
var dbname = 'mqemitter-test'
var url = 'mongodb://127.0.0.1/'

MongoClient.connect(url, { w: 1 }, function (err, client) {
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
  })
})
