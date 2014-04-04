describe 'hbase', () ->


	hbase = require '../index.coffee'

	client = hbase
		zookeeperHosts: ['localhost']
		zookeeperRoot: '/hbase'



	it 'put', (done) ->
		put = new hbase.Delete 'aaa'
		put.add
		client.put 'mrdka', put, done

