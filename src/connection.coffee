debug           = (require 'debug') 'hbase-connection'
assert          = require 'assert'
net             = require 'net'
hconstants      = require './hconstants'
Call            = require './call'
{EventEmitter}  = require 'events'
DataInputStream = require './data-input-stream'
{DataOutputStream, DataOutputBuffer} = require './output-buffer'


ProtoBuf   = require 'protobufjs'
ProtoBuf.convertFieldsToCamelCase = true
ByteBuffer = require 'protobufjs/node_modules/bytebuffer'

builderClient    = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
builderMaster    = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Master.proto")
builderAdmin    = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Admin.proto")
rpcBuilder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/RPC.proto")

# protoClient = builderClient.build()
# protoAdmin = builderAdmin.build()
rpcProto   = rpcBuilder.build()

BUILDERS =
	ClientService: builderClient
	AdminService: builderAdmin
	MasterService: builderMaster

# {ClientService, GetRequest} = protoClient
# {AdminService} = adminClient

{ConnectionHeader, RequestHeader, ResponseHeader} = rpcProto



connectionId = 0
module.exports = class Connection extends EventEmitter
	constructor: (options) ->
		assert options, "Missing options"
		assert options.host, "Missing host"
		assert options.port, "Missing port"

		@serviceName = options.service or 'ClientService'
		@id = connectionId++
		@callId = 1
		@calls = {}
		@header = null
		@socket = null
		@tcpNoDelay = no
		@tcpKeepAlive= yes
		@address =
			host: options.host
			port: options.port
		@name = "connection(#{@address.host}:#{@address.port}) id: #{@id}"
		@_connected = no
		@_socketError = null
		@_callNums = 0
		@_callTimeout = options.callTimeout or 5000 # TODO from constants
		@setupIOStreams()


	setupIOStreams: () =>
		debug "connecting to #{@name}"
		@setupConnection()
		@in = new DataInputStream @socket
		@out = new DataOutputStream @socket

		@in.on 'messages', @processMessages

		@socket.on 'connect', () =>
			debug "connected to #{@name}"
			@writeHead()
			# console.log @serviceName
			ch = new ConnectionHeader
				serviceName: @serviceName
			# console.log "XXXX", ch
			header = ch.encode().toBuffer()
			@out.writeInt header.length
			@out.write header
			@_connected = yes

			impl = (method, req, done) =>
				builder = BUILDERS[@serviceName]
				assert builder, "Invalid builder"
				reflect = builder.lookup method
				reqHeader = new RequestHeader
					callId: @callId++
					requestParam: yes
					methodName: reflect.name
				reqHeaderBuffer = reqHeader.toBuffer()

				@out.writeDelimitedBuffers reqHeaderBuffer, req.toBuffer()
				@calls[reqHeader.callId] = new Call reflect.resolvedResponseType.clazz, reqHeader, @_callTimeout, done

			@rpc = new Service @serviceName, impl

			@emit 'connect'


	processMessages: (messages) =>
		header = ResponseHeader.decode messages[0]
		unless call = @calls[header.callId]
			debug "Invalid callId #{header.callId}"
			return

		delete @calls[header.callId]
		return call.complete header.exception if header.exception

		call.complete null, call.responseClass.decode messages[1]


	writeHead: () =>
		b1 = new Buffer 1
		b1.writeUInt8 0, 0
		b2 = new Buffer [1]
		b2.writeUInt8 80, 0
		@out.write Buffer.concat [hconstants.HEADER, b1, b2]


	setupConnection: () =>
		ioFailures = 0
		timeoutFailures = 0
		@socket = net.connect @address

		@socket.setNoDelay @tcpNoDelay
		@socket.setKeepAlive @tcpKeepAlive

		@socket.on "timeout", @_handleTimeout
		@socket.on "close", @_handleClose

		# when error, response all calls error
		@socket.on "error", @_handleError

		# send ping
		#@_pingTimer = setInterval @sendPing.bind @, @pingInterval


	_handleClose: () =>
		@closed = yes
		@emit 'close'
		@socket.end()
		debug "_handleClose #{JSON.stringify arguments}"


	_handleError: (err) =>
		@_handleClose()
		debug "_handleError #{JSON.stringify arguments}"


	_handleTimeout: () ->
		debug "_handleTimeout #{JSON.stringify arguments}"


class Service
	constructor: (service, impl) ->
		builder = BUILDERS[service]
		assert builder, "Invalid builder"

		r = builder.lookup service
		client = new r.clazz impl
		r.children.forEach (child) =>
			@[child.name] = (req, done) ->
				try
					clazz = child.resolvedRequestType.clazz
					req = new clazz req if req not instanceof clazz
					client[child.name].call client, req, done
				catch err
					done err



