hbase = require '../index.coffee'

client = hbase
	zookeeperHosts: ['localhost']
	zookeeperRoot: '/hbase'

put = new hbase.Put '12345'
client._action()

