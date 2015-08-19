ProtoBuf   = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()



module.exports = class Delete
	constructor: (@row, @ts) ->
		@familyMap = {}


	deleteColumn: (cf, qualifier, timestamp) =>
		@_add cf, qualifier, timestamp, 'DELETE_ONE_VERSION'


	deleteColumns: (cf, qualifier, timestamp) =>
		@_add cf, qualifier, timestamp, 'DELETE_MULTIPLE_VERSIONS'


	deleteFamilyVersion: (cf, timestamp) =>
		@_add cf, null, timestamp, 'DELETE_FAMILY_VERSION'


	deleteFamily: (cf, timestamp) =>
		@_add cf, null, timestamp, 'DELETE_FAMILY'


	_add: (cf, qualifier, timestamp, deleteType) =>
		timestamp ?= ByteBuffer.Long.MAX_VALUE

		@familyMap[cf] ?= []
		@familyMap[cf].push {qualifier, timestamp, deleteType}


	getFields: () =>
		o =
			row: @row
			mutateType: "DELETE"
			columnValue: []

		for cf, qualifiers of @familyMap
			column =
				family: cf
				qualifierValue: qualifiers.map (qualifier) ->
					qualifier

			o.columnValue.push column

		o


	getRow: () =>
		@row



