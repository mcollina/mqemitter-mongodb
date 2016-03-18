'use strict'

var mqemitter = require('../')
var instance = mqemitter({
  url: 'mongodb://localhost/aaa'
})

instance.on('hello', function (data, cb) {
  console.log(data)
  cb()
})
