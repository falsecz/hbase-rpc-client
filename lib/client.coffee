debug            = (require 'debug') 'hbase-client'
rDebug           = (require 'debug') 'hbase-region'
zkDebug          = (require 'debug') 'zk'
zkProto          = require './zk-protobuf'
utils            = require './utils'
crypto           = require 'crypto'
async            = require 'async'
hconstants       = require './hconstants'

Connection       = require './connection'
Get              = require './get'
Put              = require './put'
Delete           = require './delete'
Increment        = require './increment'
Scan             = require('./scan').Scan

ZooKeeperWatcher = require 'zookeeper-watcher'
{EventEmitter}   = require 'events'

ProtoBuf = require("protobufjs")
ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto   = builder.build()


md5sum = (data) ->
	crypto.createHash('md5').update(data).digest('hex')



module.exports = class Client extends EventEmitter
	constructor: (options) ->
		super()

		options.zookeeperRoot = options.zookeeperRoot or "/hbase"
		if options.zookeeper and typeof options.zookeeper.quorum is "string"
			options.zookeeperHosts = options.zookeeper.quorum.split(SERVERNAME_SEPARATOR)

		@zk = new ZooKeeperWatcher
			hosts: options.zookeeperHosts
			root: options.zookeeperRoot

		@servers = {}
		@cachedRegionLocations = {}
		@rpcTimeout = 30000
		@pingTimeout = 30000
		@zkStart = "init"
		@rootRegionZKPath = options.rootRegionZKPath or '/meta-region-server'
		@prefetchRegionCacheList = {}
		@ensureZookeeperTrackers (err) =>
			@emit 'error', err if err


	_zkWatch: () =>
		@zk.unWatch @rootRegionZKPath
		@zk.watch @rootRegionZKPath, (err, value, zstat) =>
			firstStart = @zkStart isnt "done"
			if err
				setTimeout () =>
					@_zkWatch()
				, hconstants.SOCKET_RETRY_WAIT_MS

				zkDebug "[%s] [worker:%s] [hbase-client] zookeeper watch error: %s", new Date(), process.pid, err.stack
				if firstStart
					# only first start fail will emit ready event
					@zkStart = "error"
					@emit "ready", err
				return

			@zkWatchTimeoutCount = 1
			rootServer = zkProto.decodeMeta value
			unless rootServer
				console.log "Failed to parse rootServer"
				return

			@zkStart = "done"
			oldServer = @rootServer or server: hostName: 'none', port: 'none'
			@rootServer = rootServer.server

			serverName = @getServerName @rootServer
			# TODO: not needed
			@getRegionConnection serverName, (err, server) =>
				return cb err if err
				zkDebug "zookeeper start done, got new root #{serverName}, old #{oldServer?.hostName}:#{oldServer?.port}"

				# only first start success will emit ready event
				@emit "ready" if firstStart

			#@locateRegion hconstants.META_TABLE_NAME


	ensureZookeeperTrackers: (cb) =>
		return cb() if @zkStart is "done"
		@once "ready", cb
		return if @zkStart is "starting"
		@zkStart = "starting"

		@zk.once "connected", (err) =>
			if err
				@zkStart = "error"
				zkDebug "[%s] [worker:%s] [hbase-client] zookeeper connect error: %s", new Date(), process.pid, err.stack
				return @emit "ready", err

			@_zkWatch()


	getServerName: (hostname, port) ->
		if typeof hostname is 'object'
			port = hostname.port
			hostname = hostname.hostName

		"#{hostname}:#{port}"


	locateRegion: (table, row, useCache, cb) =>
		if typeof useCache is 'function'
			cb = useCache
			useCache = yes

		rDebug "locateRegion table: #{table} row: #{row}"
		table = new Buffer table unless Buffer.isBuffer table
		row = new Buffer(row or [0])

		@ensureZookeeperTrackers (err) =>
			return cb err if err

			@locateRegionInMeta table, row, useCache, cb


	locateRegionInMeta: (table, row, useCache, cb) =>
		rDebug "locateRegionInMeta table: #{table} row: #{row}"
		region = @createRegionName(table, row, '', yes)

		if utils.bufferCompare(table, hconstants.META_TABLE_NAME) is 0
			o =
				startKey: new Buffer 0
				endKey: new Buffer 0
				name: hconstants.META_REGION_NAME
				server: @getServerName @rootServer.hostName, @rootServer.port

			return cb null, o

		req =
			region:
				type: "REGION_NAME"
				value: hconstants.META_REGION_NAME
			gxt:
				row: region
				column:
					family: "info"
				closestRowBefore: yes

		@prefetchRegionCache table, () =>
			if useCache
				cachedRegion = @getCachedLocation table, row
				return cb null, cachedRegion if cachedRegion

			@getRegionConnection @rootServer.hostName, @rootServer.port, (err, server) =>
				server.rpc.Get req, (err, response) =>
					if err
						rDebug "locateRegionInMeta error: #{err}"
						return cb err

					if response?.result
						region = @_parseRegionInfo @_parseResponse response.result

					unless region.server
						err = "region for table #{table} not found"
						cb err
						return rDebug err

					@cacheLocation table, region
					cb null, region


	_parseRegionInfo: (res) ->
		return null unless Object.keys(res).length

		regionInfo = res.cols['info:regioninfo'].value
		regionInfo = regionInfo.slice regionInfo.toString().indexOf('PBUF') + 4
		regionInfo = proto.RegionInfo.decode regionInfo

		region =
			server: res.cols['info:server'].value
			startKey: regionInfo.startKey.toBuffer()
			endKey: regionInfo.endKey.toBuffer()
			name: res.row
			ts: res.cols['info:server'].timestamp.toString()

		region


	cacheLocation: (table, region) =>
		#debug "cacheLocation #{region.name} server: #{region.server}"
		@cachedRegionLocations[table] ?= {}
		@cachedRegionLocations[table][region.name] = region


	getCachedLocation: (table, row) =>
		return null unless @cachedRegionLocations[table] and Object.keys(@cachedRegionLocations[table]).length > 0
		cachedRegions = Object.keys(@cachedRegionLocations[table])

		for cachedRegion in cachedRegions
			startKey = @cachedRegionLocations[table][cachedRegion].startKey
			endKey = @cachedRegionLocations[table][cachedRegion].endKey

			if utils.bufferCompare(row, endKey) <= 0 and utils.bufferCompare(row, startKey) > 0
				rDebug "Found cached regionLocation #{cachedRegion}"
				return @cachedRegionLocations[table][cachedRegion]

		null


	printRegion: (region) ->
		o =
			startKey: region.startKey.toString()
			endKey: region.endKey.toString()
			name: region.name.toString()
			ts: region.ts.toString()
			server: region.server.toString()


	_parseResponse: (res) ->
		return null unless res?.cell?.length

		# TODO: upravit strukturu
		row = null
		cols = {}
		columns = []

		for cell in res.cell
			row = cell.row.toBuffer()
			f = cell.family.toBuffer()
			q = cell.qualifier.toBuffer()
			v = cell.value.toBuffer()
			t = cell.timestamp

			cols["#{f}:#{q}"] =
				value: v
				timestamp: t

			columns.push
				family: f
				qualifier: q
				value: v
				timestamp: t

		o =
			row: row
			cols: cols
			columns: columns

		o


	createRegionName: (table, startKey, id, newFormat) ->
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

			@getRegionConnection location.server.toString(), (err, server) =>
				return cb err if err

				if method is 'get'
					req =
						region:
							type: "REGION_NAME"
							value: location.name
						gxt: obj.getFields()

					server.rpc.Get req, (err, response) =>
						return cb err if err

						cb null, @_parseResponse response.result
				else if method in ['put', 'delete', 'increment']
					req =
						region:
							type: "REGION_NAME"
							value: location.name
						mutation: obj.getFields()

					server.rpc.Mutate req, cb
				else if method in ['checkAndPut', 'checkAndDelete']
					comparator =  new proto.BinaryComparator
						comparable:
							value: obj.value

					req =
						region:
							type: "REGION_NAME"
							value: location.name
						mutation: obj.op.getFields()
						condition:
							row: obj.row
							family: obj.family
							qualifier: obj.qualifier
							compareType: 'EQUAL'
							comparator:
								name: 'org.apache.hadoop.hbase.filter.BinaryComparator'
								serializedComparator: comparator.encode()

					server.rpc.Mutate req, cb


	_multiAction: (table, multiActions, useCache, retry, cb) =>
		if typeof useCache is 'function'
			cb = useCache
			useCache = yes
			retry = 0
		else if typeof retry is 'function'
			cb = retry
			retry = 0

		req =
			regionAction: []

		result = []
		async.each Object.keys(multiActions), (serverName, done) =>
			for region, actions of multiActions[serverName]
				operations = []

				for action in actions
					if action.method is 'get'
						operations.push gxt: action.getFields()
					else if action.method in ['put', 'delete']
						operations.push mutation: action.getFields()

				req.regionAction.push
					region:
						type: "REGION_NAME"
						value: region
					action: operations

			@getRegionConnection serverName, (err, server) =>
				return done err if err

				server.rpc.Multi req, (err, res) =>
					return done err if err

					for serverResult in res.regionActionResult
						for response in serverResult.resultOrException
							o = @_parseResponse response.result
							result.push o if o

					done()
		, (err) ->
			cb err, result


	getScanner: (table, startRow, stopRow) =>
		new Scan table, startRow, stopRow, @


	get: (table, get, cb) =>
		debug "get on table: #{table} get: #{JSON.stringify get}"
		@_action 'get', table, get, cb


	checkAndPut: (table, row, family, qualifier, value, put, cb) =>
		o =
			row: row
			family: family
			qualifier: qualifier
			value: value
			op: put

		debug "checkAndPut on table: #{table} object: #{JSON.stringify o}"

		@_action 'checkAndPut', table, o, cb


	checkAndDelete: (table, row, family, qualifier, value, del, cb) =>
		o =
			row: row
			family: family
			qualifier: qualifier
			value: value
			op: del

		debug "checkAndDelete on table: #{table} object: #{JSON.stringify o}"

		@_action 'checkAndDelete', table, o, cb


	put: (table, put, cb) =>
		debug "put on table: #{table} put: #{JSON.stringify put}"
		@_action 'put', table, put, cb


	delete: (table, del, cb) =>
		debug "delete on table: #{table} delete: #{JSON.stringify del}"
		@_action 'delete', table, del, cb


	mget: (table, rows, columns, opts, cb) =>
		return cb "Input is expected to be a non-empty array" unless Array.isArray(rows) and rows.length > 0
		debug "mget on table: #{table} #{rows.length} rows"

		if typeof columns is 'function'
			cb = columns
			opts = {}
			columns = []
		else if typeof opts is 'function'
			cb = opts
			opts = {}

		workingList = []
		for row in rows
			if row instanceof Get
				get = row
			else
				get = new Get row

				if columns
					for column in columns
						column = column.split ':'
						get.addColumn column[0], column[1]

			get.method = 'get'
			workingList.push get

		@processBatch table, workingList, true, 0, (err, results) ->
			cb err, results


	mput: (table, rows, opts, cb) =>
		return cb "Input is expected to be a non-empty array" unless Array.isArray(rows) and rows.length > 0
		debug "mput on table: #{table} #{rows.length} rows"

		if typeof columns is 'function'
			cb = columns
			opts = {}
			columns = []
		else if typeof opts is 'function'
			cb = opts
			opts = {}

		workingList = []
		for row in rows
			if row instanceof Put
				put = row
			else
				put = new Put row.row

				for column, value of row
					continue if column is 'row'

					column = column.split ':'
					put.add column[0], column[1], value

			put.method = 'put'
			workingList.push put

		@processBatch table, workingList, true, 0, (err, results) ->
			cb err, results


	mdelete: (table, rows, opts, cb) =>
		return cb "Input is expected to be a non-empty array" unless Array.isArray(rows) and rows.length > 0
		debug "mdelete on table: #{table} #{rows.length} rows"

		if typeof columns is 'function'
			cb = columns
			opts = {}
			columns = []
		else if typeof opts is 'function'
			cb = opts
			opts = {}

		workingList = []
		for row in rows
			if row instanceof Delete
				del = row
			else
				del = new Delete row

			del.method = 'delete'
			workingList.push del

		@processBatch table, workingList, true, 0, (err, results) ->
			cb err, results


	increment: (table, increment, cb) =>
		debug "increment on table: #{table} increment: #{JSON.stringify increment}"
		@_action 'increment', table, increment, cb


	incrementColumnValue: (table, row, cf, qualifier, value, cb) =>
		increment = new Increment row
		increment.add cf, qualifier, value
		@increment table, increment, cb


	mutateRow: () ->
		throw new Error 'mutateRow not implemented'


	append: () ->
		throw new Error 'append is not implemented'


	getRowOrBefore: () ->
		throw new Error 'getRowOrBefore is not implemented'


	processBatch: (table, workingList, useCache, retry, cb) =>
		if typeof useCache is 'function'
			cb = useCache
			useCache = yes

		actionsByServer = {}

		workingList.filter (item) ->
			item?

		return cb null, [] if workingList.length is 0

		async.each workingList, (row, done) =>
			@locateRegion table, row.getRow(), useCache, (err, location) ->
				return done err if err

				actionsByServer[location.server] ?= {}
				actionsByServer[location.server][location.name] ?= []
				actionsByServer[location.server][location.name].push row
				done()
		, (err) =>
			return cb err if err
			@_multiAction table, actionsByServer, useCache, retry, cb


	prefetchRegionCache: (table, cb) =>
		return cb() if @prefetchRegionCacheList[table] or utils.bufferCompare(table, hconstants.META_TABLE_NAME) is 0
		debug "prefetchRegionCache for table: #{table}"

		startRow = @createRegionName table, null, hconstants.ZEROS, no
		stopRow = @createRegionName utils.bufferIncrement(table), null, hconstants.ZEROS, no

		scan = @getScanner hconstants.META_TABLE_NAME, startRow, stopRow

		work = yes
		async.whilst () ->
			work
		, (done) =>
			scan.next (err, regionRow) =>
				return done err if err

				unless regionRow.row
					@prefetchRegionCacheList[table] = yes
					work = no
					return done()

				region = @_parseRegionInfo regionRow
				@cacheLocation table, region if region
				done()
		, (err) ->
			console.log err if err
			cb()


	getRegionConnection: (hostname, port, cb) =>
		if typeof port is 'function'
			cb = port
			serverName = hostname
			[hostname, port] = hostname.split ':'
		else
			serverName = @getServerName hostname, port

		server = @servers[serverName]
		if server
			if server.state is "ready"
				rDebug "getRegionConnection from cache (servers: #{Object.keys(@servers).length}), #{serverName}"
				cb null, server
			else
				server.on 'ready', () ->
					cb null, server
			return

		rDebug "getRegionConnection connecting to #{serverName}"
		server = new Connection(
			host: hostname
			port: port
			rpcTimeout: @rpcTimeout
			logger: @logger
		)
		server.state = "connecting"

		# cache server
		@servers[serverName] = server
		timer = null


		# handle connect timeout
		timer = setTimeout () =>
			err = "#{serverName} connect timeout, " + @rpcTimeout + " ms"
			@_handleConnectionError err, serverName, timer
			return
		, @rpcTimeout

		server.once "connect", =>
			rDebug "%s connected, total %d connections", serverName, Object.keys(@servers).length
			server.state = "ready"
			server.emit 'ready'
			clearTimeout timer
			timer = null
			cb null, server

		server.once "connectError", @_handleConnectionError.bind @, null, serverName, timer

		# TODO: connection always emit close event?
		server.once "close", @_handleConnectionClose.bind @, serverName


	_handleConnectionError: (err, serverName, timer) =>
		rDebug "_handleConnectionError server: #{serverName} msg: #{err.message}"

		if timer
			clearTimeout timer
			timer = null

		debug err if err

		server = @servers[serverName]
		delete @servers[serverName]

		# avoid 'close' and 'connect' event emit
		server.removeAllListeners()
		server.close()


	_handleConnectionClose: (serverName) =>
		rDebug "_handleConnectionClose server: #{serverName}"
		delete @servers[serverName]
		@_clearCachedLocationForServer serverName


	_clearCachedLocationForServer: (serverName) =>
		for table, regions of @cachedRegionLocations
			for regionName, region of regions
				if region.server.toString() is serverName
					delete @cachedRegionLocations[table]
					@prefetchRegionCacheList[table] = no


