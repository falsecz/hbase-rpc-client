



module.exports =
	bufferIncrement: (buffer, i) =>
		b = new Buffer buffer
		i ?= b.length - 1

		return Buffer.concat [new Buffer [1], b] if i < 0

		tmp = new Buffer [parseInt b[i]]
		tmp[0]++

		newBuffer = @bufferIncrement(b, i - 1) if tmp[0] < b[i]
		return newBuffer if newBuffer and b.length < newBuffer.length

		b[i] = tmp[0]
		b


	bufferCompare: (a, b) ->
		len1 = a.length
		len2 = b.length

		return 0 if a is b and len1 is len2

		for i in [0..len1-1]
			if a[i] isnt b[i]
				_a = if a[i] then a[i] else 0
				_b = if b[i] then b[i] else 0
				return _a - _b

		len1 - len2


