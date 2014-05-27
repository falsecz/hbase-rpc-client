assert = require 'assert'
async = require 'async'
ByteBuffer = require '../node_modules/protobufjs/node_modules/bytebuffer'
config = require './test_config'

blanket = (require 'blanket')()



describe 'hbase', () ->
	before ->
		matchesBlanket = (path) -> path.match /node_modules\/blanket/
		runningTestCoverage = Object.keys(require.cache).filter(matchesBlanket).length > 0
		console.log 'xxxx', runningTestCoverage
		if runningTestCoverage
			require('require-dir')("#{__dirname}/../lib", {recurse: true, duplicates: true})

	@_timeout = config.timeout

	tCf = 'cf1'
	tRow = '1'
	tCol = 'col'
	tVal = 'val'

	# create pre-splitted table
	# hbase org.apache.hadoop.hbase.util.RegionSplitter node-hbase-test-table HexStringSplit -c 10 -f cf1
	testTable = config.testTable
	testRows = [
			row: '111111'
			cf: tCf
			col: 'col1'
			val: 'val1'
		,
			row: '555555'
			cf: tCf
			col: 'col2'
			val: 'val2'
		,
			row: '999999'
			cf: tCf
			col: 'col3'
			val: 'val3'
		,
			row: 'aaaaaa'
			cf: tCf
			col: 'col4'
			val: 'val4'
	]
	randomValue = 'lkjhgfdsa'
	hbase = require '../index.coffee'

	client = hbase
		zookeeperHosts: config.zookeeperHosts
		zookeeperRoot: config.zookeeperRoot


	putRow = (row, cf, col, val, ts, cb) ->
		if typeof ts is 'function'
			cb = ts
			ts = null

		put = new hbase.Put row
		put.add cf, col, val, ts

		client.put testTable, put, (err, res) ->
			assert.equal err, null
			assert.equal res.processed, yes
			getRow row, cf, col, val, ts, cb

	getRow = (row, cf, col, val, ts, cb) ->
		if typeof ts is 'function'
			cb = ts
			ts = null

		get = new hbase.Get row
		get.addColumn cf, col

		client.get testTable, get, (err, res) ->
			assert.equal err, null
			assert.equal res.row, row
			assert.equal res.columns[0].family, cf
			assert.equal res.columns[0].qualifier, col
			assert.equal res.columns[0].timestamp.toString(), ts.toString() if ts
			if typeof val is 'object'
				assert.equal res.columns[0].value.toString(), val.toString()
			else
				assert.equal res.columns[0].value, val
			cb res

	deleteRow = (row, cb) ->
		del = new hbase.Delete row

		client.delete testTable, del, (err, res) ->
			assert.equal err, null
			assert.equal res.processed, yes
			rowDoesNotExist row, cb

	rowDoesNotExist = (row, cb) ->
		get = new hbase.Get row

		client.get testTable, get, (err, res) ->
			assert.equal err, null
			assert.equal res, null
			cb()

	it 'should put single row', (done) ->
		putRow testRows[0].row, testRows[0].cf, testRows[0].col, testRows[0].val, () ->
			done()

	it 'should get single row', (done) ->
		getRow testRows[0].row, testRows[0].cf, testRows[0].col, testRows[0].val, () ->
			done()

	it 'should delete row', (done) ->
		deleteRow testRows[0].row, done

	it 'should deleteColumn & deleteColumns', (done) ->
		tests = [
			(cb) ->
				putRow tRow, tCf, tCol, tVal, cb
		,
			(cb) ->
				putRow tRow, tCf, tCol, tVal + '1', cb
		,
			(cb) ->
				putRow tRow, tCf, tCol, tVal + '2', cb
		,
			(cb) ->
				del = new hbase.Delete tRow
				del.deleteColumn tCf, tCol

				client.delete testTable, del, (err, res) ->
					assert.equal err, null
					assert.equal res.processed, yes
					cb()
		,
			(cb) ->
				getRow tRow, tCf, tCol, tVal + '1', cb
		,
			(cb) ->
				del = new hbase.Delete tRow
				del.deleteColumns tCf, tCol

				client.delete testTable, del, (err, res) ->
					assert.equal err, null
					assert.equal res.processed, yes
					cb()
		,
			(cb) ->
				rowDoesNotExist tRow, cb
		]

		async.waterfall tests, () ->
			done()

	it 'should deleteFamily & deleteFamilies', (done) ->
		ts = null
		tests = [
			(cb) ->
				putRow tRow, tCf, tCol, tVal, (row) ->
					ts = row.cols["#{tCf}:#{tCol}"].timestamp
					cb()
		,
			(cb) ->
				putRow tRow, tCf, tCol, tVal + '1', (++ts).toString(), () ->
					cb()
		,
			(cb) ->
				putRow tRow, tCf, tCol, tVal + '2', (++ts).toString(), () ->
					cb()
		,
			(cb) ->
				del = new hbase.Delete tRow
				del.deleteFamilyVersion tCf, ts

				client.delete testTable, del, (err, res) ->
					assert.equal err, null
					assert.equal res.processed, yes
					cb()
		,
			(cb) ->
				getRow tRow, tCf, tCol, tVal + '1', (--ts).toString(), () ->
					cb()
		,
			(cb) ->
				del = new hbase.Delete tRow
				del.deleteFamily tCf

				client.delete testTable, del, (err, res) ->
					assert.equal err, null
					assert.equal res.processed, yes
					cb()
		,
			(cb) ->
				rowDoesNotExist tRow, cb
		]

		async.waterfall tests, () ->
			done()

	it 'should put multiple rows via array of Put objects', (done) ->
		puts = testRows.map (row) ->
			put = new hbase.Put row.row
			put.add row.cf, row.col, row.val
			put

		client.mput testTable, puts, (err, res) ->
			assert.equal err, null
			done()

	it 'should get multiple rows via array of Get objects', (done) ->
		gets = testRows.map (row) ->
			new hbase.Get row.row

		client.mget testTable, gets, (err, res) ->
			assert.equal err, null
			assert.equal res.length, testRows.length
			done()

	it 'should delete multiple rows via array of Delete objects', (done) ->
		rows = testRows.map (row) ->
			row.row

		dels = testRows.map (row) ->
			new hbase.Delete row.row

		client.mdelete testTable, dels, (err, res) ->
			assert.equal err, null
			assert.equal res.length, 0

			client.mget testTable, rows, (err, res) ->
				assert.equal err, null
				assert.equal res.length, 0
				done()

	it 'should put multiple rows via simple array', (done) ->
		puts = []
		puts = testRows.map (row) ->
			o =
				row: row.row
			o["#{row.cf}:#{row.col}"] = row.val
			o

		client.mput testTable, puts, (err, res) ->
			assert.equal err, null
			done()

	it 'should get multiple rows via simple array', (done) ->
		gets = testRows.map (row) ->
			row.row

		client.mget testTable, gets, (err, res) ->
			assert.equal err, null
			assert.equal res.length, testRows.length
			done()

	it 'should scan the table', (done) ->
		scan = client.getScanner testTable

		async.eachSeries [0..testRows.length], (i, cb) ->
			if i is testRows.length
				scan.next (err, row) ->
					assert.equal err, null
					assert.equal Object.keys(row), 0
					cb()
			else
				scan.next (err, row) ->
					assert.equal err, null
					assert.equal row.row, testRows[i].row
					cb()
		, () ->
			done()

	it 'should scan the table with startRow and stopRow', (done) ->
		scan = client.getScanner testTable, '5', '6'

		scan.next (err, row) ->
			assert.equal err, null
			assert.equal row.row, testRows[1].row

			scan.next (err, row) ->
				assert.equal err, null
				assert.equal Object.keys(row), 0
				done()

	it 'should scan the table with filter', (done) ->
		scan = client.getScanner testTable
		scan.setFilter columnPrefixFilter: prefix: testRows[2].col

		scan.next (err, row) ->
			assert.equal err, null
			assert.equal row.row, testRows[2].row
			assert.equal row.cols["#{testRows[2].cf}:#{testRows[2].col}"].value, testRows[2].val

			scan.next (err, row) ->
				assert.equal err, null
				assert.equal Object.keys(row), 0
				done()

	it 'should scan the table with filterList', (done) ->
		scan = client.getScanner testTable
		fl = new hbase.FilterList
		fl.addFilter columnPrefixFilter: prefix: testRows[2].col
		scan.setFilter fl

		scan.next (err, row) ->
			assert.equal err, null
			assert.equal row.row, testRows[2].row
			assert.equal row.cols["#{testRows[2].cf}:#{testRows[2].col}"].value, testRows[2].val

			scan.next (err, row) ->
				assert.equal err, null
				assert.equal Object.keys(row), 0
				done()

	it 'should scan the table and convert result to array', (done) ->
		scan = client.getScanner testTable

		scan.toArray (err, res) ->
			assert.equal err, null

			for i, row of testRows
				assert.equal res[i].row, testRows[i].row
				assert.equal res[i].cols["#{testRows[i].cf}:#{testRows[i].col}"].value, testRows[i].val

			done()

	it 'should scan the table with filterList consisting of multiple filterLists', (done) ->
		scan = client.getScanner testTable

		f1 =
			singleColumnValueFilter:
				columnFamily: 'cf1'
				columnQualifier: 'col2'
				compareOp: 'EQUAL'
				comparator:
					substringComparator:
						substr: '2'
				filterIfMissing: yes
				latestVersionOnly: yes

		f2 =
			singleColumnValueFilter:
				columnFamily: 'cf1'
				columnQualifier: 'col3'
				compareOp: 'EQUAL'
				comparator:
					substringComparator:
						substr: '3'
				filterIfMissing: yes
				latestVersionOnly: yes

		fl1 = new hbase.FilterList
		fl2 = new hbase.FilterList
		fl3 = new hbase.FilterList 'MUST_PASS_ONE'

		fl1.addFilter f1
		fl2.addFilter f2

		fl3.addFilter fl1
		fl3.addFilter fl2

		scan.setFilter fl3

		scan.toArray (err, res) ->
			assert.equal err, null

			assert.equal res[0].row, testRows[1].row
			assert.equal res[0].cols["#{testRows[1].cf}:#{testRows[1].col}"].value, testRows[1].val

			assert.equal res[1].row, testRows[2].row
			assert.equal res[1].cols["#{testRows[2].cf}:#{testRows[2].col}"].value, testRows[2].val
			done()

	it 'should delete multiple rows via simple array', (done) ->
		rows = testRows.map (row) ->
			row.row

		client.mdelete testTable, rows, (err, res) ->
			assert.equal err, null
			assert.equal res.length, 0

			client.mget testTable, rows, (err, res) ->
				assert.equal err, null
				assert.equal res.length, 0
				done()

	it 'should checkAndPut', (done) ->
		putRow testRows[0].row, testRows[0].cf, testRows[0].col, testRows[0].val, () ->
			put = new hbase.Put testRows[0].row
			put.add testRows[0].cf, testRows[0].col, randomValue

			client.checkAndPut testTable, testRows[0].row, testRows[0].cf, testRows[0].col, testRows[0].val, put, (err, res) ->
				assert.equal err, null
				assert.equal res.processed, yes
				done()

	it 'should checkAndDelete', (done) ->
		del = new hbase.Delete testRows[0].row

		client.checkAndDelete testTable, testRows[0].row, testRows[0].cf, testRows[0].col, randomValue, del, (err, res) ->
			assert.equal err, null
			assert.equal res.processed, yes

			get = new hbase.Get testRows[0].row

			client.get testTable, get, (err, res) ->
				assert.equal err, null
				assert.equal res, null
				done()

	it 'should increment & incrementColumnValue', (done) ->
		inc = 4
		b = new ByteBuffer 8
		b.writeLong 65
		b = b.toBuffer()

		tests = [
			(cb) ->
				putRow tRow, tCf, tCol, b, () ->
					cb()
		,
			(cb) ->
				increment = new hbase.Increment tRow
				increment.add tCf, tCol, inc

				client.increment testTable, increment, (err, res) ->
					assert.equal err, null
					val = res.result.cell[0].value.toBuffer()
					assert.equal hbase.utils.bufferCompare(val, b), inc
					cb()
		,
			(cb) ->
				client.incrementColumnValue testTable, tRow, tCf, tCol, inc, (err, res) ->
					assert.equal err, null
					val = res.result.cell[0].value.toBuffer()
					assert.equal hbase.utils.bufferCompare(val, b), inc * 2
					cb()
		,
			(cb) ->
				deleteRow tRow, cb
		]

		async.waterfall tests, () ->
			done()







