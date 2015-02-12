
var mongoEmitter  = require('./')
  , test          = require('tape').test
  , abstractTests = require('mqemitter/abstractTest.js')

abstractTests({
    builder: function(opts) {
      opts = opts || {}
      opts.url = 'mongodb://127.0.0.1/mqemitter-test?auto_reconnect'
      return mongoEmitter(opts)
    }
  , test: test
})
