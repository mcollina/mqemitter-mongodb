'use strict'

var mongoEmitter = require('./')
var test = require('tape').test
var abstractTests = require('mqemitter/abstractTest.js')

abstractTests({
  builder: function (opts) {
    opts = opts || {}
    opts.url = 'mongodb://127.0.0.1/mqemitter-test'

    return mongoEmitter(opts)
  },
  test: test
})
