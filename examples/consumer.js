'use strict'

const mqemitter = require('../')
const instance = mqemitter({
  url: 'mongodb://localhost/aaa'
})

instance.on('hello', function (data, cb) {
  console.log(data)
  cb()
})
