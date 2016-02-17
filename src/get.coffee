

module.exports = class Get
	constructor: (@row) ->
		@tr =
			from: null
			to: null
		@familyMap = {}
		@maxVersions = 1


	addColumn: (cf, qualifier) =>
		@familyMap[cf] ?= []
		@familyMap[cf].push qualifier if qualifier
		@


	setMaxVersions: (maxVersions) =>
		maxVersions = 1 if maxVersions <= 0
		@maxVersions = maxVersions
		@


	setTimeRange: (minStamp, maxStamp) =>
		@tr =
			from: minStamp
			to: maxStamp
		@


	getFields: () =>
		o =
			row: @row
			timeRange: @tr
			column: []
			maxVersions: @maxVersions

		for cf, qualifiers of @familyMap
			o.column.push
				family: cf
				qualifier: qualifiers.map (qualifier) ->
					qualifier

		o


	getRow: () =>
		@row



