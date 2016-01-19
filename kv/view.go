package kv

import (
	"encoding/json"
	"reflect"

	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/reflection"
)

type view struct {
	collection *collection
	filter     []interface{}
	limit      int64
	skip       int64
	sort       []string
}

func (v *view) copy() *view {
	return &view{
		collection: v.collection,
		filter:     v.filter,
		limit:      v.limit,
		skip:       v.skip,
		sort:       v.sort,
	}
}

// Count returns the number of items that match the set conditions.
func (v *view) Count() (int64, error) {
	var c, err = v.cursor(false)
	if err != nil {
		return 0, err
	}
	var count int64
	for c.next() {
		count++
	}
	return count, nil
}

// One fetches the first result within the result set.
func (v *view) One(result interface{}) error {
	var c, err = v.cursor(false)
	if err != nil {
		return err
	}
	if !c.next() {
		return errNotFound
	}
	return json.Unmarshal(c.value(), result)
}

// All fetches all results within the result set.
func (v *view) All(result interface{}) error {
	rval := reflect.ValueOf(result)
	if rval.Kind() != reflect.Ptr || rval.Elem().Kind() != reflect.Slice {
		return errNotSliceAddr
	}

	var c, err = v.cursor(false)
	if err != nil {
		return err
	}

	slice := rval.Elem()
	slice = slice.Slice(0, slice.Cap())
	elemType := slice.Type().Elem()

	var i = 0
	var meta = reflection.GetMeta(elemType)

	for ; c.next(); i++ {
		if slice.Len() == i {
			elemp := reflect.New(elemType)
			item := elemp.Interface()
			err = v.unmarshal(c, item, meta)
			if err != nil {
				return err
			}
			slice = reflect.Append(slice, reflect.ValueOf(item).Elem())
			slice = slice.Slice(0, slice.Cap())
		} else {
			item := slice.Index(i).Addr().Interface()
			err = v.unmarshal(c, item, meta)
			if err != nil {
				return err
			}
		}
	}

	rval.Elem().Set(slice.Slice(0, i))

	return nil
}

func (v *view) unmarshal(c *cursor, result interface{}, meta *reflection.Meta) error {
	var err = json.Unmarshal(c.value(), result)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = reflection.GetMeta(result)
	}
	meta.SetID(result, string(c.key()))
	return nil
}

// Limit defines the maximum number of results in this set.
func (v *view) Limit(n int64) data.Result {
	var t = v.copy()
	t.limit = n
	return t
}

// Skip ignores first *n* results.
func (v *view) Skip(n int64) data.Result {
	var t = v.copy()
	t.skip = n
	return t
}

// Sort results by given fields.
func (v *view) Sort(fields ...string) data.Result {
	var t = v.copy()
	t.sort = fields
	return t
}

// Cursor executes query and returns cursor capable of going over all the results.
func (v *view) Cursor() (data.Cursor, error) {
	return v.cursor(false)
}

func (v *view) cursor(writeable bool) (*cursor, error) {
	var db = v.collection.db
	var tx, err = db.Begin(writeable)
	if err != nil {
		return nil, err
	}

	bucket, err := tx.Bucket(v.collection.name, false)
	if bucket == nil || err != nil {
		if err != nil {
			return nil, err
		}
		return nil, errNotFound
	}

	var iter Iter

	if len(v.filter) > 0 {
		var lp = lookup{
			collection: v.collection,
			tx:         tx,
		}
		if lp.isSuitable(v.filter) {
			var keys = lp.find(v.filter)
			iter = KeysIter(bucket.Cursor(), keys, v.limit, v.skip)
		}
	}

	if iter == nil {
		iter = FilterIter(bucket.Cursor(), v.filter, v.limit, v.skip)
	}

	if len(v.sort) > 0 {
		iter = SortIter(iter, v.sort)
	}

	var result = &cursor{
		view: v,
		tx:   tx,
		bkt:  bucket,
		iter: iter,
	}

	return result, nil
}
