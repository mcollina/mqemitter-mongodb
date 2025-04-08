'use strict'

const mqemitter = require('../')
const instance = mqemitter({
  url: 'mongodb://localhost/aaa'
})

setInterval(function () {
  instance.emit({ topic: 'hello', payload: 'world' })
}, 1000)
