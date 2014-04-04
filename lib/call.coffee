debug = (require 'debug') 'hbase-call'



module.exports = class Call
	constructor: (@responseClass, @header, @timeout, @cb) ->
		@called = no
		@startTime = new Date

		@timer = setTimeout () =>
			debug "operation #{@header.callId} timedout after #{@timeout}ms"
			@called = yes
			@cb 'timedout'
		, @timeout


	complete: (err, data) =>
		return if @called
		debug "operation #{@header.callId} completed. Took: #{new Date - @startTime}ms"
		clearTimeout @timer
		@cb err, data


