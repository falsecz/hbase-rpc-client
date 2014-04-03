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

