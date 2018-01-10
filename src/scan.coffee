debug    = (require 'debug') 'hbase-client'
utils    = require './utils'
async    = require 'async'
ProtoBuf = require("protobufjs")

ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Filter.proto")
proto = builder.build()



module.exports.getFilter = getFilter = (filter) ->
	FilterList = require './filter-list'

	if filter instanceof FilterList
		filterList =  new proto.FilterList filter.get()

		o =
			name: 'org.apache.hadoop.hbase.filter.FilterList'
			serializedFilter: filterList.encode()
		return o

	#exception kdyz jich bude vic
	filterName = Object.keys(filter)[0]
	filterNameUpper = "#{filterName[0].toUpperCase()}#{filterName[1..]}"

	throw new Error "Invalid filter #{filterNameUpper}" unless proto[filterNameUpper]

	if filterNameUpper in ['SingleColumnValueFilter']
		filter[filterName].comparator = getFilter filter[filterName].comparator

	o =
		name: "org.apache.hadoop.hbase.filter.#{filterNameUpper}"
	serialized = 'serializedFilter'
	serialized = 'serializedComparator' if filterNameUpper.indexOf('Comparator') >= 0
	o[serialized] = new proto[filterNameUpper](filter[filterName]).encode()
	o


module.exports.Scan = class Scan
	constructor: (@table, @startRow, @stopRow, @client) ->
		@closed = no
		@numCached = 100
		@cached = []
		@server = null
		@location = null
		@timeout = null
		@row = 0
		@reversed = no


	setReversed: (reversed = yes) =>
		@reversed = !!reversed
		@


	setFilter: (filter) =>
		@filter = getFilter filter

		return false unless @filter
		yes


	# fetch data and return closest row
	_getData: (nextStartRow, cb) =>
		return cb null, {} if @closed

		@getServerAndLocation @table, nextStartRow, (err, @server, @location) =>
			return cb err if err

			req =
				region:
					type: "REGION_NAME"
					value: @location.name
				numberOfRows: @numCached
				scan:
					reversed: @reversed

			if @scannerId
				req.scannerId = @scannerId
			else
				req.scan.startRow = @startRow if @startRow
				req.scan.stopRow = @stopRow if @stopRow
			req.scan.filter = @filter if @filter

			@row++
			region = @location.name.toString()
			debug "scan on: #{@table} row: #{@row} region: #{region} startRow: #{@startRow} stopRow: #{@stopRow}"
			@server.rpc.Scan req, (err, response) =>
				return cb err if err

				@_processResponse response, cb


	_processResponse: (response, cb) =>
		nextRegion = yes
		@nextStartRow = null
		@scannerId = response.scannerId
		clearTimeout @timeout if @timeout
		@timeout = setTimeout @close, response.ttl

		len = response.results.length
		# we didn't finish scanning of the current region
		if len is @numCached
			nextRegion = no
		# or there are no more regions to scan
		if (@location.endKey.length is 0 and not @reversed) or (@location.startKey.length is 0 and @reversed)
			nextRegion = no
		# or stopRow was contained in the current region
		if not @reversed and @stopRow and utils.bufferCompare(@location.endKey, new Buffer @stopRow) > 0 and len isnt @numCached
			nextRegion = no
		# or stopRow was contained in the current region for reversed scan
		if @reversed and @stopRow and utils.bufferCompare(@location.startKey, new Buffer @stopRow) < 0 and len isnt @numCached
			nextRegion = no

		# we need to go to another region
		if len < @numCached
			@nextStartRow = utils.bufferIncrement @location.endKey
			@nextStartRow = utils.bufferDecrement @location.startKey if @reversed
			@nextStartRow = @nextStartRow.toString()

		# go to another region
		if nextRegion
			@closeScan @server, @location, @scannerId
			@server = @location = @scannerId = null
			return @_getData @nextStartRow, cb if len is 0

		@cached = response.results.map (result) =>
			@client._parseResponse result

		# no more results anywhere.. close the scan
		@close() if @cached.length is 0

		cb()


	getServerAndLocation: (table, startRow, cb) =>
		return cb null, @server, @location if @server and @location

		{ locateRegion } = @client
		locateRegion = @client.locatePreviousRegion if @reversed

		locateRegion table, startRow, (err, location) =>
			return cb err if err

			@client.getRegionConnection location.server.toString(), (err, server) ->
				return cb err if err

				cb null, server, location


	next: (cb) =>
		# still have some results in cache
		return cb null, (@cached.splice 0, 1)[0] if @cached.length

		startRow = @nextStartRow
		if @reversed
			startRow ?= new Buffer []
		else
			startRow ?= @startRow

		@_getData startRow, (err) =>
			return cb err if err
			return cb null, {} unless @cached.length
			cb null, (@cached.splice 0, 1)[0]


	closeScan: (server, location, scannerId) ->
		return unless location

		req =
			region:
				type: "REGION_NAME"
				value: location.name
			closeScanner: yes
			scannerId: scannerId

		server.rpc.Scan req, (err, response) ->


	close: () =>
		return if @closed
		@closeScan @server, @location, @scannerId
		@cached = []
		@closed = yes


	each: (f, cb) =>
		work = yes
		async.whilst () ->
			work
		, (done) =>
			@next (err, row) ->
				return done err if err

				unless row.row
					work = no
					return process.nextTick done

				return f null, row, done if f.length is 3

				f null, row
				process.nextTick done
		, (err) ->
			return cb err if cb
			f err


	toArray: (cb) =>
		out = []
		@each (err, row) ->
			return cb err, out unless row

			out.push row



