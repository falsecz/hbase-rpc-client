ProtoBuf = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/ZooKeeper.proto")
proto = builder.build()

exports.decodeMeta = (data) ->
	magic = data.toString().indexOf "PBUF"
	data = data.slice magic + 5
	proto.MetaRegionServer.decode data




