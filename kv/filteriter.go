package kv

import (
	"encoding/json"

	"github.com/gocontrib/log"
)

// FilterIter creates filtered iterator.
func FilterIter(cursor Cursor, filter []interface{}, limit, skip int64) Iter {
	return &filterIter{
		cursor:    cursor,
		filterFn:  MakeFilterFn(filter),
		hasFilter: len(filter) > 0,
		limit:     limit,
		skip:      skip,
	}
}

type filterIter struct {
	cursor      Cursor
	filterFn    FilterFn
	hasFilter   bool
	limit       int64
	skip        int64
	count       int64
	initialized bool
	closed      bool
	// current pair
	k []byte
	v []byte
}

func (it *filterIter) Key() []byte {
	return it.k
}

func (it *filterIter) Value() []byte {
	return it.v
}

func (it *filterIter) Next() (bool, error) {
	if it.closed {
		return false, nil
	}

	// support limit
	if it.limit > 0 && it.count >= it.limit {
		it.close()
		return false, nil
	}

	var k []byte
	var v []byte
	if !it.initialized {
		it.initialized = true
		tk, tv, err := it.init()
		if err != nil {
			return false, err
		}
		k, v = tk, tv
	} else {
		k, v = it.cursor.Next()
	}

	if k == nil {
		it.close()
		return false, nil
	}

	var err = it.filter(&k, &v)
	if err != nil {
		log.Error("filter failed: %v", err)
		return false, err
	}

	if k == nil {
		it.close()
		return false, nil
	}

	it.k = k
	it.v = v

	return true, nil
}

func (it *filterIter) init() ([]byte, []byte, error) {
	k, v := it.cursor.First()
	// skip first n docs
	if it.skip > 0 {
		var skip = it.skip
		for k != nil && skip > 0 {
			var err = it.filter(&k, &v)
			if err != nil {
				debug.Err("filter", err)
				return nil, nil, err
			}
			if k == nil {
				break
			}
			k, v = it.cursor.Next()
			skip--
		}
	}
	return k, v, nil
}

func (it *filterIter) filter(k, v *[]byte) error {
	if it.hasFilter {
		for *k != nil {
			var d map[string]interface{}
			var err = json.Unmarshal(*v, &d)
			if err != nil {
				log.Error("json.Unmarshal failed: %v", err)
				return err
			}
			if it.filterFn(string(*k), d) {
				break
			}
			tk, tv := it.cursor.Next()
			*k = tk
			*v = tv
		}
	}
	if *k != nil {
		it.count++
	}
	return nil
}

func (it *filterIter) close() {
	if !it.closed {
		it.closed = true
		it.k = nil
		it.v = nil
	}
}
