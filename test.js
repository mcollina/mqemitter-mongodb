'use strict'

var mongoEmitter = require('./')
var test = require('tape').test
var abstractTests = require('mqemitter/abstractTest.js')
var clean = require('mongo-clean')
var url = 'mongodb://127.0.0.1/mqemitter-test'

clean(url, function (err, db) {
  if (err) {
    throw err
  }

  db.close()

  abstractTests({
    builder: function (opts) {
      opts = opts || {}
      opts.url = url

      return mongoEmitter(opts)
    },
    test: test
  })
})
