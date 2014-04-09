ProtoBuf = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()

#console.log proto.MutationProto

module.exports = class Delete
	constructor: (@row, @ts) ->
		@familyMap = {}


	deleteColumn: (cf, qualifier, timestamp) =>
		@_add cf, qualifier, timestamp, 'DELETE_ONE_VERSION'


	deleteColumns: (cf, qualifier, timestamp) =>
		@_add cf, qualifier, timestamp, 'DELETE_MULTIPLE_VERSIONS'


	deleteFamily: (cf, timestamp) =>
		@_add cf, undefined, timestamp, 'DELETE_FAMILY'


	deleteFamilies: (cf, timestamp) =>
		@_add cf, undefined, timestamp, 'DELETE_FAMILY_VERSIONS'


	_add: (cf, qualifier, timestamp, deleteType) =>
		timestamp ?= ByteBuffer.Long.MAX_VALUE

		@familyMap[cf] ?= []
		@familyMap[cf].push {qualifier, timestamp, deleteType}


	getFields: () =>
		o =
			row: @row
			mutateType: "DELETE"
			columnValue: []

		for cf, qualifierValue of @familyMap
			o.columnValue.push
				family: cf
				qualifierValue: qualifierValue

		o


	getRow: () =>
		@row



