debug = require('debug')('hbase:data_input_stream')
Readable = require('readable-stream').Readable
#Bytes = require './util/bytes'
#WritableUtils = require './writable_utils'


module.exports = class DataInputStream
	constructor: (io) ->
		@in = io
		if typeof io.read isnt 'function'
			@in = new Readable()
			@in.wrap(io)

		@bytearr = new Buffer(80)


	read: (b, cb) =>
		@in.read b, 0, b.length


	readBytes: (size, cb) =>
		buf = @in.read size
		debug 'readBytes: %d size, Got %s, socket total read bytes: %d', size, buf ? 'Buffer' : null, this.in.bytesRead
		if buf is null
			return @in.once 'readable', @readBytes.bind @, size, cb

		cb null, buf
