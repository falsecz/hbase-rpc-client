ProtoBuf = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'
builder = ProtoBuf.loadProtoFile("ZooKeeper.proto")
proto = builder.build()

zookeeper = require 'zookeeper-client'

zk = zookeeper "10.11.0.192:2181"

x = new proto.MetaRegionServer
	server:
		host_name: "regionserver"
		port: 60020
# console.log x
console.log x.encode().toBuffer() #toString()

# zk.getChildren '/hbase', no, () ->
# 	console.log arguments
zk.get '/hbase/meta-region-server', no, (err, info, data) ->
	console.log data.length
	magic = data.toString().indexOf "PBUF"
	data = data.slice magic + 4

	console.log proto.MetaRegionServer.decode data



# zk.create "/hera/brokers/broker", 123124, zk.ZOO_SEQUENCE | zk.ZOO_EPHEMERAL, (err, path) ->
# 	console.log arguments
#
# zk.getChildren "/hera/brokers", no, (err, childs) ->
# 	console.log arguments
#
# zk.volunteer "bender0000000000", (err, o) ->
# 	console.log arguments
