Readable       = require('readable-stream').Readable
ByteBuffer     = require 'bytebuffer'
{EventEmitter} = require 'events'



module.exports = class DataInputStream extends EventEmitter
	constructor: (io) ->
		@in = io
		if typeof io.read isnt 'function'
			@in = new Readable()
			@in.wrap(io)

		@bytearr = new Buffer 80
		@buffer = new Buffer 0

		@awaitBytes = 0
		@in.on 'data', @processData


	processData: (data) =>
		data = new Buffer(0) unless data

		@buffer = Buffer.concat [@buffer, data]
		# expecting Int
		return if @awaitBytes is 0 and @buffer.length < 4

		unless @awaitBytes
			@awaitBytes = @buffer.readUInt32BE 0
			@buffer = @buffer.slice 4

		return if @awaitBytes and @awaitBytes > @buffer.length

		message = @buffer.slice 0, @awaitBytes
		@buffer = @buffer.slice @awaitBytes
		@awaitBytes = 0

		@processMessage message
		@processData() if @buffer.length > 0


	processMessage: (message) =>
		payload = ByteBuffer.wrap message

		readDelimited = () ->
			headerLen = payload.readVarint32()
			header = payload.slice payload.offset , payload.offset + headerLen
			payload.offset += headerLen
			header.toBuffer()

		messages = []
		messages.push readDelimited() while payload.remaining()
		@.emit 'messages', messages



