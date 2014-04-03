


module.exports = (options) ->
	Client = require('./lib/client')
	new Client options

module.exports.Put = require './lib/put'



