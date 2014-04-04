


module.exports = (options) ->
	Client = require('./lib/client')
	new Client options

module.exports.Put = require './lib/put'
module.exports.Get = require './lib/get'
module.exports.Delete = require './lib/delete'

