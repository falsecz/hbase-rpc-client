debug = (require 'debug') 'hbase-client'
utils = require './utils'
async = require 'async'

ProtoBuf = require("protobufjs")
ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Filter.proto")
proto = builder.build()


module.exports.getFilter = getFilter = (filter) ->
	FilterList = require './filter-list'

	if filter instanceof FilterList
		filterList =  new proto.FilterList filter.get()

		name: 'org.apache.hadoop.hbase.filter.FilterList'
		serializedFilter: filterList.encode()
	else
		#exception kdyz jich bude vic
		filterName = Object.keys(filter)[0]
		filterNameUpper = "#{filterName[0].toUpperCase()}#{filterName[1..]}"

		unless proto[filterNameUpper]
			console.log "Invalid filter #{filterNameUpper}"
			return false

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
				scan: {}

			if @scannerId
				req.scannerId = @scannerId
			else if @startRow and @stopRow
				req.scan =
					startRow: @startRow
					stopRow: @stopRow

			req.scan.filter = @filter if @filter

			@row++
			debug "scan on table: #{@table} row: #{@row} region: #{@location.name.toString()} startRow: #{@startRow} stopRow: #{@stopRow}"
			@server.rpc.Scan req, (err, response) =>
				return cb err if err

				nextRegion = yes
				@nextStartRow = null
				@scannerId = response.scannerId
				clearTimeout @timeout if @timeout
				@timeout = setTimeout @close, response.ttl

				# we didn't finish scanning of the current region
				if response.results.length is @numCached \
					# or there are no more regions to scan
					or @location.endKey.length is 0 \
					# or stopRow was contained in the current region
					or @stopRow and (utils.bufferCompare(@location.endKey, new Buffer @stopRow) > 0) and response.results.length isnt @numCached
						nextRegion = no

				# we need to go to another region
				if response.results.length < @numCached
					@nextStartRow = utils.bufferIncrement @location.endKey
					@nextStartRow = @nextStartRow.toString()

				# go to another region
				if nextRegion
					@closeScan @server, @location, @scannerId
					@server = @location = @scannerId = null
					return @_getData @nextStartRow, cb if response.results.length is 0

				@cached = []
				for result in response.results
					@cached.push @client._parseResponse result

				# no more results anywhere.. close the scan
				@close() if @cached.length is 0

				cb()


	getServerAndLocation: (table, startRow, cb) =>
		return cb null, @server, @location if @server and @location

		@client.locateRegion table, startRow, (err, location) =>
			return cb err if err

			@client.getRegionConnection location.server.toString(), (err, server) =>
				return cb err if err

				cb null, server, location


	next: (cb) =>
		# still have some results in cache
		return cb null, (@cached.splice 0, 1)[0] if @cached.length

		startRow = @nextStartRow
		startRow ?= @startRow

		@_getData startRow, (err) =>
			return cb err if err
			return cb null, {} unless @cached.length
			cb null, (@cached.splice 0, 1)[0]


	closeScan: (server, location, scannerId) =>
		req =
			region:
				type: "REGION_NAME"
				value: location.name
			closeScanner: yes
			scannerId: scannerId

		server.rpc.Scan req, (err, response) =>


	close: () =>
		return if @closed
		@closeScan @server, @location, @scannerId
		@closed = yes


	each: (f, cb) =>
		work = yes
		async.whilst () ->
			work
		, (done) =>
			@next (err, row) =>
				return done err if err

				unless row.row
					work = no
					return process.nextTick done

				f row
				return process.nextTick done
		, (err) ->
			cb err


	toArray: (cb) =>
		out = []
		@each (row) ->
			out.push row
		, (err) ->
			cb err, out



