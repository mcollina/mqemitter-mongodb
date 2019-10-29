mqemitter-mongodb&nbsp;&nbsp;[![Build Status](https://travis-ci.org/mcollina/mqemitter-mongodb.png)](https://travis-ci.org/mcollina/mqemitter-mongodb)
=================

MongoDB powered [MQEmitter](http://github.com/mcollina/mqemitter).

See [MQEmitter](http://github.com/mcollina/mqemitter) for the actual
API.

[![js-standard-style](https://raw.githubusercontent.com/feross/standard/master/badge.png)](https://github.com/feross/standard)

Install
-------

```bash
$ npm install mqemitter-mongodb --save
```

Example
-------

```js
var mongodb = require('mqemitter-mongodb')
var mq = mongodb({
  url: 'mongodb://127.0.0.1/mqemitter?auto_reconnect'
})
var msg  = {
  topic: 'hello world',
  payload: 'or any other fields'
}

mq.on('hello world', function (message, cb) {
  // call callback when you are done
  // do not pass any errors, the emitter cannot handle it.
  cb()
})

// topic is mandatory
mq.emit(msg, function () {
  // emitter will never return an error
})
```

API
-----
### MQEmitterMongoDB([opts])

Create a new instance of mqemitter-mongodb.

Options:

* `url`: a mongodb endpoint url
* `database`: a mongodb database name, by default it comes from the uri
* `mongo`: options for mongodb client
* `db`: a db instance of mongodb (instead of url)


Acknowledgements
----------------

Code ported from [Ascoltatori](http://github.com/mcollina/ascoltatori).

License
-------

MIT
