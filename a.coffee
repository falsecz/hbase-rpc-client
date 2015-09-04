Connection = require './src/connection'

#
# c = new Connection
# 	service: 'AdminService'
# 	host: 'c-sencha-c08'
# 	port: 60020
#
# c.on "close", (e) ->
# 	console.log 'close', e
#
#
# c.once "connect", =>
# 	console.log "connected"
# 	r =
# 		region:
# 			type: "REGION_NAME"
# 			value: "page_info_stats,,1440768095991.cf3d9cc16d9e5477cea37f8d6c7bd42e."
#
# 	console.log c.rpc.GetRegionInfo r, () ->
# 		console.log JSON.stringify arguments
#



m = new Connection
	service: 'MasterService'
	host: 'c-sencha-s01'
	port: 60000


m.once "connect", =>

	console.log m.rpc.ListTableNamesByNamespace namespaceName: 'default', (err, result) ->
		return console.log err if err
		for table in result.tableName
			console.log table.namespace.toBuffer().toString(), table.qualifier.toBuffer().toString()
