ProtoBuf   = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'
hconstants = require './hconstants'

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/ZooKeeper.proto")
proto = builder.build()



exports.decodeMeta = (data) ->
	return if data[0] isnt hconstants.MAGIC

	len = ByteBuffer.wrap(data).readInt32(1)
	dataLength = data.length - hconstants.MAGIC_SIZE - hconstants.ID_LENGTH_SIZE - len
	dataOffset = hconstants.MAGIC_SIZE + hconstants.ID_LENGTH_SIZE + len

	data = data.slice(dataOffset + 4, dataOffset + dataLength)

	proto.MetaRegionServer.decode data




