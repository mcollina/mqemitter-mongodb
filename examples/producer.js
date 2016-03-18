'use strict'

var mqemitter = require('../')
var instance = mqemitter({
  url: 'mongodb://localhost/aaa'
})

setInterval(function () {
  instance.emit({ topic: 'hello', payload: 'world' })
}, 1000)
