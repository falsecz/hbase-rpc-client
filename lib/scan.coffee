debug = (require 'debug') 'hbase-client'
utils = require './utils'




# TODO: timer kterej kdyz ten scanner nezavres, tak ho closnes manualne (60s tusim)
module.exports = class Scan
	constructor: (@table, @startRow, @stopRow, @filter, @client) ->
		@closed = no
		@numCached = 50
		@cached = []
		@server = null
		@location = null
		@timeout = null
		@row = 0


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



