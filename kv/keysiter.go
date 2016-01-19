package kv

// KeysIter makes iterator  over specified keys.
func KeysIter(it Cursor, keys []string, limit, skip int64) Iter {
	return &keysIter{
		cursor: it,
		keys:   keys,
		limit:  limit,
		skip:   skip,
	}
}

type keysIter struct {
	cursor      Cursor
	keys        []string
	limit       int64
	skip        int64
	idx         int
	count       int64
	initialized bool
	closed      bool
	// current pair
	k []byte
	v []byte
}

func (it *keysIter) Key() []byte {
	return it.k
}

func (it *keysIter) Value() []byte {
	return it.v
}

func (it *keysIter) Next() (bool, error) {
	if it.closed {
		return false, nil
	}

	// support limit
	if it.limit > 0 && it.count >= it.limit {
		it.close()
		return false, nil
	}

	if !it.initialized {
		it.initialized = true
		var skip = it.skip
		for skip > 0 && it.idx < len(it.keys) {
			// check key
			k := []byte(it.keys[it.idx])
			it.idx++
			k, _ = it.cursor.Seek(k)
			if k != nil {
				skip--
			}
		}
	} else {
		it.idx++
	}

	for it.idx < len(it.keys) {
		k := []byte(it.keys[it.idx])
		k, v := it.cursor.Seek(k)
		if k != nil {
			it.k = k
			it.v = v
			it.count++
			break
		}
		it.idx++
	}

	if it.idx >= len(it.keys) {
		it.close()
		return false, nil
	}

	return true, nil
}

func (it *keysIter) close() {
	if !it.closed {
		it.closed = true
		it.k = nil
		it.v = nil
	}
}
