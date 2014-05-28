{getFilter} = require './scan'



module.exports = class FilterList
	constructor: (@operator, filter) ->
		@operator = 'MUST_PASS_ALL' unless @operator in ['MUST_PASS_ALL', 'MUST_PASS_ONE']

		@filters = []

		if filter
			filter = getFilter filter
			return no unless filter

			@filters.push filter


	addFilter: (filter) =>
		filter = getFilter filter
		return no unless filter

		@filters.push filter


	get: () =>
		operator: @operator
		filters: @filters

