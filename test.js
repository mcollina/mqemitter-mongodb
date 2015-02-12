
var mongoEmitter  = require('./')
  , test          = require('tape').test
  , abstractTests = require('mqemitter/abstractTest.js')

abstractTests({
    builder: function(opts) {
      opts = opts || {}
      opts.url = 'mongodb://127.0.0.1/mqemitter-test?auto_reconnect'

      // idiot quirk because mongo is too slow in delivering my message
      var emitter = mongoEmitter(opts)
        , emit    = emitter.emit

      emitter.emit = function(obj, cb) {
        emit.call(emitter, obj, function(err) {
          setTimeout(function() {
            if (cb) {
              cb(err)
            }
          }, 50)
        })
      }

      return emitter
    }
  , test: test
})
