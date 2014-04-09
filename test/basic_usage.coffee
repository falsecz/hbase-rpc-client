assert = require 'assert'

# create pre-splitted table
# hbase org.apache.hadoop.hbase.util.RegionSplitter mrdka HexStringSplit -c 10 -f cf1

describe 'hbase', () ->
	@_timeout = 2000

	testTable = 'mrdka'
	testRows = [
			row: '111111'
			cf: 'cf1'
			col: 'col'
			val: 'val1'
		,
			row: '555555'
			cf: 'cf1'
			col: 'col'
			val: 'val2'
		,
			row: '999999'
			cf: 'cf1'
			col: 'col'
			val: 'val3'
		,
			row: 'aaaaaa'
			cf: 'cf1'
			col: 'col'
			val: 'val4'
	]
	hbase = require '../index.coffee'

	client = hbase
		#zookeeperHosts: ['10.11.0.192']
		zookeeperHosts: ['localhost']
		zookeeperRoot: '/hbase'

	it 'should put single row', (done) ->
		put = new hbase.Put testRows[0].row
		put.add testRows[0].cf, testRows[0].col, testRows[0].val
		client.put testTable, put, (err, res) ->
			assert.equal err, null
			assert.equal res.processed, yes
			done()

	it 'should get single row', (done) ->
		get = new hbase.Get testRows[0].row
		client.get testTable, get, (err, res) ->
			assert.equal err, null
			assert.equal res[0].row, testRows[0].row
			assert.equal res[0].family, testRows[0].cf
			assert.equal res[0].qualifier, testRows[0].col
			assert.equal res[0].value, testRows[0].val
			done()

	it 'should delete row', (done) ->
		del = new hbase.Delete testRows[0].row
		client.delete testTable, del, (err, res) ->
			assert.equal err, null
			assert.equal res.processed, yes

			get = new hbase.Get testRows[0].row
			client.get testTable, get, (err, res) ->
				assert.equal err, null
				assert.equal res.length, 0
				done()

	it 'should put multiple rows via array of Put objects', (done) ->
		puts = []
		for row in testRows
			put = new hbase.Put row.row
			put.add row.cf, row.col, row.val
			puts.push put

		client.mput testTable, puts, (err, res) ->
			assert.equal err, null
			done()

	it 'should get multiple rows via array of Get objects', (done) ->
		gets = []
		for row in testRows
			get = new hbase.Get row.row
			gets.push get

		client.mget testTable, gets, (err, res) ->
			assert.equal err, null
			assert.equal res.length, testRows.length
			done()

	it 'should delete multiple rows via array of Delete objects', (done) ->
		rows = []
		dels = []
		for row in testRows
			rows.push row.row
			del = new hbase.Delete row.row
			dels.push del

		client.mdelete testTable, dels, (err, res) ->
			assert.equal err, null
			assert.equal res.length, 0

			client.mget testTable, rows, (err, res) ->
				assert.equal err, null
				assert.equal res.length, 0
				done()

	it 'should put multiple rows via simple array', (done) ->
		puts = []
		for row in testRows
			o =
				row: row.row
			o["#{row.cf}:#{row.col}"] = row.val
			puts.push o

		client.mput testTable, puts, (err, res) ->
			assert.equal err, null
			done()

	it 'should get multiple rows via simple array', (done) ->
		gets = []
		for row in testRows
			gets.push row.row

		client.mget testTable, gets, (err, res) ->
			assert.equal err, null
			assert.equal res.length, testRows.length
			done()

	it 'should scan the table', (done) ->
		scan = client.getScanner testTable
		scan.next (err, res) ->
			assert.equal err, null
			assert.equal res.row, testRows[0].row

			scan.next (err, res) ->
				assert.equal err, null
				assert.equal res.row, testRows[1].row

				scan.next (err, res) ->
					assert.equal err, null
					assert.equal res.row, testRows[2].row

					scan.next (err, res) ->
						assert.equal err, null
						assert.equal res.row, testRows[3].row

						scan.next (err, res) ->
							assert.equal err, null
							assert.equal Object.keys(res), 0
							done()

	it 'should delete multiple rows via simple array', (done) ->
		rows = []
		for row in testRows
			rows.push row.row

		client.mdelete testTable, rows, (err, res) ->
			assert.equal err, null
			assert.equal res.length, 0

			client.mget testTable, rows, (err, res) ->
				assert.equal err, null
				assert.equal res.length, 0
				done()










