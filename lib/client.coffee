ZooKeeperWatcher = require 'zookeeper-watcher'
{EventEmitter} = require 'events'
zkProto = require './zk-protobuf'
Connection = require './connection'
debugzk = (require 'debug') 'zk'
crypto = require 'crypto'


md5sum = (data) ->
	crypto.createHash('md5').update(data).digest('hex')

SERVERNAME_SEPARATOR = ","
META_TABLE_NAME = new Buffer 'hbase:meta'
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
				oldServer = @rootServerName or server: hostName: 'none', port: 'none'
				@rootServerName = rootServer

				serverName = @getServerName @rootServerName.server
				@servers[serverName] = new Connection
					host: rootServer.server.hostName
					port: rootServer.server.port
				debugzk "zookeeper start done, got new root #{serverName}, old #{oldServer?.server?.hostName}:#{oldServer?.server?.port}"

				# only first start success will emit ready event
				@servers[serverName].on 'connect', () =>
					@emit "ready" if firstStart

				#@locateRegion META_TABLE_NAME


	bufferEqual: (a, b) ->
		return no unless Buffer.isBuffer a
		return no unless Buffer.isBuffer b
		return no if a.length isnt b.length

		for i, v of a
			return no if a[i] isnt b[i]

		yes


	getServerName: (server) ->
		"#{server.hostName}:#{server.port}"


	locateRegion: (table, row, useCache, cb) =>
		table = new Buffer table unless Buffer.isBuffer table
		row = new Buffer(row or 0)

		@ensureZookeeperTrackers (err) =>
			return cb err if err

			if @bufferEqual table, META_TABLE_NAME
				console.log 'rootServer', @rootServerName
				@locateRegionInMeta table, row, useCache, cb
			else
				console.log 'nehledam v meta'
				@locateRegionInMeta table, row, useCache, cb


	locateRegionInMeta: (table, row, useCache, cb) =>
		region = @createRegionName('mrdka', '', '1396544002751', yes).toString()
		@servers[@getServerName @rootServerName.server].rpc.Get
			region:
				type: "REGION_NAME"
				value: region
			gxt:
				row: region
				column:
					family: "cf1"
				closestRowBefore: yes

		, (err, response) ->
			console.log arguments


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
		@locateRegion 'hbase:meta', '', useCache, () =>
			cb()


	prefetchRegionCache: (table, row, cb) =>
		startRow = ''


	getRegionConnection: (hostname, port, cb) =>
		server = @servers[@getServerName rsName]
		readyEvent = "getRegionConnection:" + rsName + ":ready"
		if server and server.state is "ready"
			debug "getRegionConnection from cache(%d), %s", @serversLength, rsName
			return callback(null, server)

		# debug('watting `%s` event', readyEvent);
		@once readyEvent, callback
		return if server
		server = new Connection(
			host: hostname
			port: port
			rpcTimeout: @rpcTimeout
			logger: @logger
		)
		server.state = "connecting"

		# cache server
		@servers[rsName] = server
		@serversLength++
		timer = null
		handleConnectionError = handleConnectionError = (err) =>
			if timer
				clearTimeout timer
				timer = null
			delete @servers[rsName]

			@serversLength--

			# avoid 'close' and 'connect' event emit.
			server.removeAllListeners()
			server.close()
			debug err.message
			@emit readyEvent, err
			return


		# handle connect timeout
		timer = setTimeout () =>
			err = new errors.ConnectionConnectTimeoutException(rsName + " connect timeout, " + @rpcTimeout + " ms")
			handleConnectionError err
			return
		, @rpcTimeout
		server.once "connect", =>
			clearTimeout timer
			timer = null

			# should getProtocolVersion() first to check version
			server.getProtocolVersion null, null, (err, version) =>
				server.state = "ready"
				return @emit(readyEvent, err) if err
				version = version.toNumber()
				debug "%s connected, protocol: %s, total %d connections", rsName, version, @serversLength
				@emit readyEvent, null, server

		server.once "connectError", handleConnectionError

		# TODO: connection always emit close event?
		server.once "close", @_handleConnectionClose.bind(@ rsName)










