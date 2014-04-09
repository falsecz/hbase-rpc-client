debug = (require 'debug') 'hbase-client'


ProtoBuf = require("protobufjs")
ProtoBuf.convertFieldsToCamelCase = true
builder = ProtoBuf.loadProtoFile("#{__dirname}/../proto/Client.proto")
proto = builder.build()




# TODO: timer kterej kdyz ten scanner nezavres, tak ho closnes manualne (60s tusim)
module.exports = class Scan
	constructor: (@table, @startRow, @stopRow, @filter, @client) ->
		@closed = no
		@numCached = 10
		@cached = []
		@server = null
		@location = null


	# fetch data and return closest row
	_getData: (nextStartRow, cb) =>
		return cb "Scan is closed" if @closed

		@getServerAndLocation @table, nextStartRow, (err, @server, @location) =>
			return cb err if err

			req =
				region:
					type: "REGION_NAME"
					value: location.name
				numberOfRows: @numCached
				scan: {}

			if @scannerId
				req.scannerId = @scannerId
			else if @startRow and @stopRow
				req.scan =
					startRow: @startRow
					stopRow: @stopRow

			@server.rpc.Scan req, (err, response) =>
				return cb err if err

				nextRegion = yes
				@nextStartRow = null
				@scannerId = response.scannerId

				# we didn't finish scanning of the current region
				if response.results.length is @numCached \
					# or there are no more regions to scan
					or location.endKey.length is 0 \
					# or stopRow was contained in the current region
					or @stopRow and (@client.bufferCompare(location.endKey, new Buffer @stopRow) > 0) and response.results.length isnt @numCached
						nextRegion = no

				# we need to go to another region
				if response.results.length < @numCached
					@nextStartRow = @bufferIncrement new Buffer location.endKey
					@nextStartRow = @nextStartRow.toString()

				# go to another region
				if nextRegion
					@closeScan @server, @location, @scannerId
					@server = @location = @scannerId = null
					return @_getData @nextStartRow, cb if response.results.length is 0

				@cached = []
				for result in response.results
					for cell in result.cell
						@cached.push @client.parseResponse cell

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


	bufferIncrement: (buffer, i) =>
		i ?= buffer.length - 1

		return Buffer.concat [new Buffer [1], buffer] if i < 0

		tmp = new Buffer [parseInt buffer[i]]
		tmp[0]++

		newBuffer = @bufferIncrement(buffer, i - 1) if tmp[0] < buffer[i]
		return newBuffer if newBuffer and buffer.length < newBuffer.length

		buffer[i] = tmp[0]
		buffer


	next: (cb) =>
		debug "scan on table: #{@table} startRow: #{@startRow} stopRow: #{@stopRow}"

		# still have some results in cache
		if @cached.length > 1
			cb null, @cached[0]
			return @cached.splice 0, 1

		startRow = @nextStartRow
		startRow ?= @startRow

		@_getData startRow, (err) =>
			return cb err if err

			return cb null, {} unless @cached.length

			cb null, @cached[0]
			return @cached.splice 0, 1


	closeScan: (server, location, scannerId) =>
		req =
			region:
				type: "REGION_NAME"
				value: location.name
			closeScanner: yes
			scannerId: scannerId

		@server.rpc.Scan req, (err, response) =>


	close: () =>
		@closeScan
		@closed = yes



