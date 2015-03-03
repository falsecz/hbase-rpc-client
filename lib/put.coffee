ProtoBuf   = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()



module.exports = class Put
	constructor: (@row, @ts) ->
		@familyMap = {}


	add: (cf, qualifier, value, timestamp) =>
		timestamp ?= @ts
		timestamp ?= ByteBuffer.Long.MAX_VALUE

		@familyMap[cf] ?= []
		unless typeof value is 'string' or Buffer.isBuffer value
			throw new Error "Invalid value type, only strings and buffers are allowed.
				Row: '#{@row}' cf: '#{cf}:#{qualifier}' value: #{value}"
		@familyMap[cf].push {qualifier, value, timestamp}


	getFields: () =>
		o =
			row: @row
			mutateType: "PUT"
			columnValue: []

		for cf, qualifierValue of @familyMap
			o.columnValue.push
				family: cf
				qualifierValue: qualifierValue

		o


	getRow: () =>
		@row





