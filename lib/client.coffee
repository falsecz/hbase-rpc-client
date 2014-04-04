ZooKeeperWatcher = require 'zookeeper-watcher'
{EventEmitter} = require 'events'
zkProto = require './zk-protobuf'
Connection = require './connection'
Get = require './get'
debugzk = (require 'debug') 'zk'
debug = (require 'debug') 'hbase-client'
crypto = require 'crypto'

ProtoBuf = require("protobufjs")
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()

md5sum = (data) ->
	crypto.createHash('md5').update(data).digest('hex')

SERVERNAME_SEPARATOR = ","
META_TABLE_NAME = new Buffer 'hbase:meta'
META_REGION_NAME = new Buffer 'hbase:meta,,1'
MAGIC = 255
MD5_HEX_LENGTH = 32
net = require 'net'

module.exports = class Client extends EventEmitter
	constructor: (options) ->
		super()

		options.zookeeperRoot = options.zookeeperRoot or "/hbase"
		options.zookeeperHosts = options.zookeeper.quorum.split(SERVERNAME_SEPARATOR) if options.zookeeper and typeof options.zookeeper.quorum is "string"

		@zk = new ZooKeeperWatcher
			hosts: options.zookeeperHosts
			root: options.zookeeperRoot

		@servers = {}
		@serversLength = 0
		@cachedRegionLocations = {}
		@rpcTimeout = 30000
		@pingTimeout = 30000
		@zkStart = "init"
		@rootRegionZKPath = options.rootRegionZKPath or '/meta-region-server'
		@ensureZookeeperTrackers (err) =>
			@emit 'error', err if err


	ensureZookeeperTrackers: (cb) =>
		return cb() if @zkStart is "done"
		@once "ready", cb
		return if @zkStart is "starting"
		@zkStart = "starting"

		@zk.once "connected", (err) =>
			if err
				@zkStart = "error"
				debugzk "[%s] [worker:%s] [hbase-client] zookeeper connect error: %s", new Date(), process.pid, err.stack
				return @emit "ready", err
			@zk.unWatch @rootRegionZKPath
			@zk.watch @rootRegionZKPath, (err, value, zstat) =>
				firstStart = @zkStart isnt "done"
				if err
					debugzk "[%s] [worker:%s] [hbase-client] zookeeper watch error: %s", new Date(), process.pid, err.stack
					if firstStart
						# only first start fail will emit ready event
						@zkStart = "error"
						@emit "ready", err
					return

				rootServer = zkProto.decodeMeta value
				@zkStart = "done"
				oldServer = @rootServer or server: hostName: 'none', port: 'none'
				@rootServer = rootServer.server

				serverName = @getServerName @rootServer
				@getRegionConnection @rootServer.hostName, @rootServer.port, (err, server) =>
					return cb err if err
					debugzk "zookeeper start done, got new root #{serverName}, old #{oldServer?.server?.hostName}:#{oldServer?.server?.port}"

					# only first start success will emit ready event
					@emit "ready" if firstStart

				#@locateRegion META_TABLE_NAME


	bufferCompare: (a, b) ->
		if a.length > b.length
			return -1
		else if a .length < b.length
			return 1

		for i, v of a
			if a[i] isnt b[i]
				return a[i] - b[i]

		0


	getServerName: (hostname, port) ->
		if typeof hostname is 'object'
			port = hostname.port
			hostname = hostname.hostname

		"#{hostname}:#{port}"


	locateRegion: (table, row, useCache, cb) =>
		debug "locateRegion table: #{table} row: #{row}"
		table = new Buffer table unless Buffer.isBuffer table
		row = new Buffer(row or 0)

		@ensureZookeeperTrackers (err) =>
			return cb err if err

			if @bufferCompare table, META_TABLE_NAME is 0
				console.log 'rootServer', @rootServer
				@locateRegionInMeta table, row, useCache, cb
			else
				@locateRegionInMeta table, row, useCache, cb


	locateRegionInMeta: (table, row, useCache, cb) =>
		debug "locateRegionInMeta table: #{table} row: #{row}"
		region = @createRegionName(table, row, '', yes)
		req =
			region:
				type: "REGION_NAME"
				value: META_REGION_NAME
			gxt:
				row: region
				column:
					family: "info"
				closestRowBefore: yes

		if useCache
			cachedRegion = @getCachedLocation table, row
			return cb null, cachedRegion if cachedRegion

		@getRegionConnection @rootServer.hostName, @rootServer.port, (err, server) =>
			server.rpc.Get req, (err, response) =>
				if err
					debug "locateRegionInMeta error: #{err}"
					return cb err

				region = {}
				if response?.result
					for res in response.result.cell
						qualifier = res.qualifier.toBuffer().toString()

						if qualifier is 'server'
							region.region = res.value.toBuffer()

						if qualifier is 'regioninfo'
							b = res.value.toBuffer()
							regionInfo = b.slice b.toString().indexOf('PBUF') + 4
							regionInfo = proto.RegionInfo.decode regionInfo
							region.startKey = regionInfo.startKey.toBuffer()
							region.endKey = regionInfo.endKey.toBuffer()
							region.name = res.row.toBuffer()
							region.ts = res.timestamp

				unless region.region
					err = "region for table #{table} not found"
					cb err
					return debug err

				@cacheLocation table, region
				cb null, region


	cacheLocation: (table, region) =>
		@cachedRegionLocations[table] ?= {}
		@cachedRegionLocations[table][region.name] = region


	getCachedLocation: (table, row) =>
		return null unless @cachedRegionLocations[table] and Object.keys(@cachedRegionLocations[table]).length > 0
		cachedRegions = Object.keys(@cachedRegionLocations[table])

		for cachedRegion in cachedRegions
			startKey = @cachedRegionLocations[table][cachedRegion].startKey
			endKey = @cachedRegionLocations[table][cachedRegion].endKey

			if @bufferCompare(row, endKey) <= 0 and @bufferCompare(row, startKey) is 1
				debug "Found cached regionLocation #{cachedRegion}"
				return @cachedRegionLocations[table][cachedRegion].region

		null


	parseResponse: (res) =>
		o =
			family: res.family.toBuffer().toString()
			qualifier: res.qualifier.toBuffer().toString()
			value: res.value.toBuffer().toString()


	createRegionName: (table, startKey, id, newFormat) =>
		table = new Buffer table unless Buffer.isBuffer table
		startKey = new Buffer(startKey or 0)
		id = new Buffer(id?.toString() or 0)
		delim = new Buffer ','
		b = Buffer.concat [table, delim, startKey, delim, id]
		md5 = new Buffer(md5sum b)

		delim = new Buffer '.'
		return Buffer.concat [b, delim, md5, delim] if newFormat

		b


	_action: (method, table, obj, useCache, retry, cb) =>
		if typeof useCache is 'function'
			cb = useCache
			useCache = yes
			retry = 0
		else if typeof retry is 'function'
			cb = retry
			retry = 0

		@locateRegion table, obj.row, useCache, (err, location) =>
			return cb err if err

			[hostname, port] = location.region.toString().split ':'
			@getRegionConnection hostname, port, (err, server) =>
				return cb err if err

				if method is 'get'
					req =
						region:
							type: "REGION_NAME"
							value: location.name
						gxt: obj.getFields()

					result = []
					server.rpc.Get req, (err, response) =>
						return cb err if err

						for res in response.result.cell
							result.push = @parseResponse res

						cb null, result
				else if method in ['put', 'delete']
					req =
						region:
							type: "REGION_NAME"
							value: location.name
						mutation: obj.getFields()

					result = []
					server.rpc.Mutate req, cb


	get: (table, get, cb) =>
		@_action 'get', table, get, cb


	put: (table, put, cb) =>
		@_action 'put', table, put, cb


	delete: (table, del, cb) =>
		@_action 'delete', table, del, cb


	prefetchRegionCache: (table, row, cb) =>
		startRow = ''


	getRegionConnection: (hostname, port, cb) =>
		serverName = @getServerName hostname, port
		server = @servers[serverName]
		if server and server.state is "ready"
			debug "getRegionConnection from cache (servers: #{@serversLength}), #{serverName}"
			return cb null, server

		return cb null, server if server

		server = new Connection(
			host: hostname
			port: port
			rpcTimeout: @rpcTimeout
			logger: @logger
		)
		server.state = "connecting"

		# cache server
		@servers[serverName] = server
		@serversLength++
		timer = null
		handleConnectionError = handleConnectionError = (err) =>
			if timer
				clearTimeout timer
				timer = null
			delete @servers[serverName]

			@serversLength--

			# avoid 'close' and 'connect' event emit.
			server.removeAllListeners()
			server.close()
			debug err.message


		# handle connect timeout
		timer = setTimeout () =>
			err = "#{serverName} connect timeout, " + @rpcTimeout + " ms"
			handleConnectionError err
			return
		, @rpcTimeout
		server.once "connect", =>
			debug "%s connected, total %d connections", serverName, @serversLength
			server.state = "ready"
			clearTimeout timer
			timer = null
			cb null, server


		server.once "connectError", handleConnectionError

		# TODO: connection always emit close event?
		#server.once "close", @_handleConnectionClose.bind(@, serverName)










