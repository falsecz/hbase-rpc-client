hbase = require '../index.coffee'

client = hbase
	zookeeperHosts: ['localhost']
	zookeeperRoot: '/hbase'


del = new hbase.Delete 'aaa'
del.deleteColumns 'cf1', 'col'
client.delete 'mrdka', del, () ->
	console.log 'test out', arguments

