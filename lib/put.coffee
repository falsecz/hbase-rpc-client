ProtoBuf = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()

#console.log proto.MutationProto

module.exports = class Put
	constructor: (rowKey) ->
		console.log rowKey





