util = require 'util'
net = require 'net'
ProtoBuf = require("protobufjs")
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'
builder = ProtoBuf.loadProtoFile("Client.proto")
rpcBuilder = ProtoBuf.loadProtoFile("RPC.proto")


proto = builder.build()
rpcProto = rpcBuilder.build()

{ClientService, GetRequest} = proto
{ConnectionHeader, RequestHeader, ResponseHeader} = rpcProto

impl = (method, req, callback) ->
	reflect = builder.lookup method

	client = net.connect 60020, "10.11.0.192", () ->
		console.log 'Connexted'
		client.writeInt = (i) ->
			v = new Buffer 4
			v.writeInt32BE i, 0
			console.log "iiii", i
			client.write v

		client.writeWithLength = (b) ->
			client.writeInt b.length
			client.write b

		client.on 'data', (data) ->
			console.log "CTU"
			# console.log data

			console.log "message length:", len = data.readUInt32BE(0)
			# console.log "payload:",
			payload = data.slice 4

			payload = ByteBuffer.wrap payload
			headerLen = payload.readVarint32()
			headerLenSize = ByteBuffer.calculateVarint32 headerLen
			header = payload.slice headerLenSize , headerLenSize + headerLen

			# console.log "header len": headerLen
			# console.log "header": payload.toBuffer()
			header = ResponseHeader.decode header
			console.log	"header": header.toBuffer()


			part2 = payload.slice headerLenSize + headerLen
			responseLen = part2.readVarint32()
			responseLenSize = ByteBuffer.calculateVarint32 responseLen
			response = part2.toBuffer().slice 0, responseLen
			# console.log response


			# console.log "response len": responseLen
			# console.log "response len size": responseLenSize
			# reflect.resolvedResponseType.clazz

			r =  reflect.resolvedResponseType.clazz.decode response
			return callback null, r















		v = new Buffer 1
		v.writeUInt8 0, 0

		a = new Buffer 1
		a.writeUInt8 80, 0
		header = Buffer.concat [new Buffer("HBas"), v, a]

		client.write header

		ch = new ConnectionHeader
			service_name: "ClientService"
		header = ch.encode().toBuffer()

		client.writeWithLength header

		#end of handshake

		reqh = new RequestHeader
			call_id: 3
			request_param: yes
			method_name: reflect.name

		a = reqh.toBuffer()
		o = req.toBuffer()

		adelimlen = ByteBuffer.calculateVarint32(a.length)
		odelimlen = ByteBuffer.calculateVarint32(o.length)

		client.writeInt a.length + o.length + adelimlen  + odelimlen

		ab = new ByteBuffer adelimlen
		ab.writeVarint32 a.length

		ob = new ByteBuffer odelimlen
		ob.writeVarint32 o.length


		client.write ab.toBuffer()
		client.write a
		client.write ob.toBuffer()
		client.write o


# {GetRequest} = proto


# console.log Object.keys ClientService.Get

class Service
	constructor: (service, impl) ->
		r = builder.lookup service
		client = new r.clazz impl
		r.children.forEach (child) =>
			@[child.name] = (req, done) ->
				clazz = child.resolvedRequestType.clazz
				req = new clazz req if req not instanceof clazz
				client[child.name].call client, req, done

class ClientService extends Service
	constructor: (impl) ->
		super 'ClientService', impl


cs = new ClientService impl
cs.Get
	region:
		type: "REGION_NAME"
		value: "mrdka,,1395046506490.0bbb1b55f4fbfd716fca778837466515."
	gxt:
		row: "radek1"
		column: family: "c"

, (err, response) ->
	return console.log err if err
	console.log response

	for cell in response.result.cell
		console.log cell.row.toBuffer().toString(), cell.family.toBuffer().toString()
		, cell.qualifier.toBuffer().toString(), cell.value.toBuffer().toString()


return
#
# TableName = x.build 'TableName'
# tn = new TableName
# 	namespace: new Buffer "ns"
# 	qualifier: new Buffer "mrdka"
#
# TableSchema = x.build 'TableSchema'
# ts = new TableSchema
# 	table_name: tn
#
#
# CreateTableRequest = x.build 'CreateTableRequest'
# ct = new CreateTableRequest
# 	table_schema: ts

RegionSpecifier = x.build 'RegionSpecifier'
# rs =

Column = x.build 'Column'
# c =

Get = x.build 'Get'
# get1 =

# console.log get1.encode().toBuffer()
# return
#   repeated Column column = 2;
#   repeated NameBytesPair attribute = 3;
#   optional Filter filter = 4;
#   optional TimeRange time_range = 5;
#   optional uint32 max_versions = 6 [default = 1];
#   optional bool cache_blocks = 7 [default = true];
#   optional uint32 store_limit = 8;
#   optional uint32 store_offset = 9;
# }

GetResponse = x.build 'GetResponse'

GetRequest = x.build 'GetRequest'
gr = new proto.GetRequest
	region: new proto.RegionSpecifier
		type: RegionSpecifier.RegionSpecifierType.REGION_NAME
		# value: "hbase:meta,,1"
		value: "mrdka,,1395046506490.0bbb1b55f4fbfd716fca778837466515."
	gxt: new proto.Get
		row: "radek1"
		# row: "mrdka,,99999999999999"
		# row: "mrdka,,99999999999999"
		column: new Column family: "c"
		closest_row_before: yes


# console.log get
# gr.encode()

# return
#

# param = (org.apache.hadoop.hbase.protobuf.generated.ClientProtos$GetRequest) region {
#   type: REGION_NAME
#   value: "hbase:meta,,1"
# }
# get {
#   row: "fb_posts,,99999999999999"
#   column {
#     family: "info"
#   }
#   closest_row_before: tr...
#
#
#


ms = x.build().ClientService

net = require 'net'
client = net.connect 60020, "10.11.0.192", () ->
	console.log 'Connexted'
	client.writeInt = (i) ->
		v = new Buffer 4
		v.writeInt32BE i, 0
		console.log "iiii", i
		client.write v

	client.writeWithLength = (b) ->
		client.writeInt b.length
		client.write b

	v = new Buffer 1
	v.writeUInt8 0, 0

	a = new Buffer 1
	a.writeUInt8 80, 0
	header = Buffer.concat [new Buffer("HBas"), v, a]

	client.write header

	ConnectionHeader = RPC.build 'ConnectionHeader'
	# console.log ConnectionHeader

	ch = new ConnectionHeader
		service_name: "ClientService"
		# service_name: "AdminService"
	header = ch.encode().toBuffer()

	client.writeWithLength header



	console.log "pisu"

	# return

	s = new ms (method, req, callback) ->
		# console.log this.Get.toString()
		# console.log method.fqn()
		# console.log ms.G
		return

		console.log "Call #{method}"
		# console.log req.encode().toBuffer()
		RequestHeader = RPC.build 'RequestHeader'
		reqh = new RequestHeader
			call_id: 3
			request_param: yes
			method_name: "Get" # "sGet" # "CreateTable" #method
		# console.log req

		# console.log "x--- ", req.encode()
		# return
		# console.log '-', 9, "08001a034765742001"
		# console.log 'a', reqh.toBuffer().length, reqh.toBuffer().toString 'hex'
		# console.log '-', 57, "0a110801120d68626173653a6d6574612c2c3112240a1866625f706f7374732c2c393939393939393939393939393912060a04696e666f5801"
		# console.log 'b', req.toBuffer().length, req.toBuffer().toString 'hex'
		# a = new Buffer "08001a034765742001", "hex"

		# o = new Buffer "0a110801120d68626173653a6d6574612c2c3112240a1866625f706f7374732c2c393939393939393939393939393912060a04696e666f5801", "hex"

		a = reqh.toBuffer()
		o = req.toBuffer()

		adelimlen = ByteBuffer.calculateVarint32(a.length)
		odelimlen = ByteBuffer.calculateVarint32(o.length)

		client.writeInt a.length + o.length + adelimlen  + odelimlen

		ab = new ByteBuffer adelimlen
		ab.writeVarint32 a.length

		ob = new ByteBuffer odelimlen
		ob.writeVarint32 o.length


		client.write ab.toBuffer()
		client.write a
		client.write ob.toBuffer()
		client.write o

		# client.writeInt reqh.toBuffer().length # + req.toBuffer().length
#
# 		client.write reqh.toBuffer()
# 		# client.write req.toBuffer()



	# s.CreateTable ct, () ->
	s.Get gr, () ->
		console.log 'xeeeee', arguments
	#
	#
	#
	#
	#
ResponseHeader = RPC.build 'ResponseHeader'

client.on 'data', (data) ->
	# console.log "CTU"

	console.log "message length:", len = data.readUInt32BE(0)
	# console.log "payload:",
	payload = data.slice 4

	payload = ByteBuffer.wrap payload
	headerLen = payload.readVarint32()
	headerLenSize = ByteBuffer.calculateVarint32 headerLen
	header = payload.slice headerLenSize , headerLenSize + headerLen

	# console.log "header len": headerLen
	# console.log "header": payload.toBuffer()
	header = ResponseHeader.decode header
	console.log	"header": header.toBuffer()


	part2 = payload.slice headerLenSize + headerLen
	responseLen = part2.readVarint32()
	responseLenSize = ByteBuffer.calculateVarint32 responseLen
	response = part2.toBuffer().slice 0, responseLen
	# console.log response


	# console.log "response len": responseLen
	# console.log "response len size": responseLenSize
	r =  GetResponse.decode response
	for cell in r.result.cell
		console.log cell.row.toBuffer().toString(), cell.family.toBuffer().toString(), cell.qualifier.toBuffer().toString(), cell.value.toBuffer().toString()


client.on 'end', ()->
  console.log 'client disconnected'
