module.exports = (options) ->
	Client = require('./client')
	new Client options

module.exports.Put = require './put'
module.exports.Get = require './get'
module.exports.Delete = require './delete'
module.exports.Increment = require './increment'
module.exports.Scan = require('./scan').Scan
module.exports.FilterList = require './filter-list'
module.exports.utils = require './utils'

