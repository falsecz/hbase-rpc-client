ProtoBuf   = require("protobufjs")
ByteBuffer = require 'bytebuffer'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()

module.exports = class Increment
	constructor: (@row, @ts) ->
		@familyMap = {}


	add: (cf, qualifier, value, timestamp) =>
		timestamp ?= ByteBuffer.Long.MAX_VALUE

		b = new ByteBuffer 8
		b.writeLong ByteBuffer.Long.fromString("#{value}")

		@familyMap[cf] ?= []
		@familyMap[cf].push {qualifier, value: b.toBuffer(), timestamp}


	getFields: () =>
		o =
			row: @row
			mutateType: 'INCREMENT'
			columnValue: []

		for cf, qualifierValue of @familyMap
			o.columnValue.push
				family: cf
				qualifierValue: qualifierValue

		o


	getRow: () =>
		@row





