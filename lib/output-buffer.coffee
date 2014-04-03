ByteBuffer = require 'protobufjs/node_modules/bytebuffer'


class DataOutputStream
	constructor: (@out) ->
		@written = 0


	write: (b, offset, length) =>
		b = new Buffer b unless Buffer.isBuffer b

		length ?= b.length
		b = b.slice offset, offset + length if offset
		@out.write b
		console.log 'write ' + length, b
		@written += length


	writeByte: (b) =>
		(if isNaN b then b = new Buffer b[0] else b = new Buffer [b]) unless Buffer.isBuffer b
		@write b


	writeInt: (i) =>
		b = new Buffer(4)
		b.writeInt32BE i, 0
		@write b


	writeDelimitedBuffers: (buffers...) =>
		length = 0
		varInt = []
		for i, buffer of buffers
			varInt[i] = ByteBuffer.calculateVarint32 buffer.length
			length += varInt[i] + buffer.length

		@writeInt length
		for i, buffer of buffers
			bb = new ByteBuffer varInt[i]
			bb.writeVarint32 buffer.length
			@write bb.toBuffer()
			@write buffer




class DataOutputBuffer extends DataOutputStream
	constructor: () ->
		super()

module.exports.DataOutputBuffer = DataOutputBuffer
module.exports.DataOutputStream = DataOutputStream
