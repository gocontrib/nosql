package kv

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/gocontrib/nosql/reflection"
)

var stringType = reflect.TypeOf("")

type idxmeta struct {
	name      string // name of index bucket
	jsonField string
	getter    reflection.Getter
}

type collectionIdx struct {
	store *store
	name  string
}

func (c *collectionIdx) getmeta(target interface{}) []idxmeta {
	var s = c.store
	s.Lock()
	defer s.Unlock()

	if s.idxmeta == nil {
		s.idxmeta = make(map[reflect.Type][]idxmeta)
	}

	t, ok := target.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(target)
	}

	m, ok := s.idxmeta[t]
	if !ok {
		m = c.makemeta(t)
		s.idxmeta[t] = m
	}

	return m
}

func (c *collectionIdx) makemeta(t reflect.Type) []idxmeta {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	var meta []idxmeta

	for i := 0; i < t.NumField(); i++ {
		var f = t.Field(i)
		if f.Type != stringType {
			continue
		}

		var name = f.Name
		var tag = f.Tag.Get("json")
		if len(tag) > 0 {
			var spec = strings.Split(tag, ",")
			name = spec[0]
		}
		if name == "id" || name == "_id" {
			continue
		}

		var idx = idxmeta{
			name:      "idx_" + c.name + "_" + name,
			jsonField: name,
			getter:    reflection.MakeGetter(f),
		}
		meta = append(meta, idx)
	}

	return meta
}

func (c *collectionIdx) update(tx Tx, id string, doc interface{}, old map[string]interface{}) error {
	for _, info := range c.getmeta(doc) {
		var name = info.jsonField

		var idx Bucket
		var err error

		// remove old index
		if old != nil {
			if idx == nil {
				idx, err = tx.Bucket(info.name, true)
				if err != nil {
					return err
				}
			}

			err = c.remove(idx, id, name, nil, old)
			if err != nil {
				return err
			}
		}

		var val = info.getter(doc).(string)
		// insert new index
		if len(val) > 0 {

			if idx == nil {
				idx, err = tx.Bucket(info.name, true)
				if err != nil {
					return err
				}
			}

			err = c.insert(idx, id, val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *collectionIdx) insert(idx Bucket, id, value string) error {
	var k = []byte(value)
	v, err := idx.Get(k)
	if err != nil {
		return err
	}
	if v != nil {
		var keys = unmarshalKeys(v)
		keys = append(keys, id)
		return idx.Set(k, keys.marshal())
	}
	return idx.Set(k, keys{id}.marshal())
}

func (c *collectionIdx) remove(idx Bucket, id, name string, value interface{}, old map[string]interface{}) error {
	if value == nil {
		v, ok := old[name]
		if !ok {
			return nil
		}
		value = v
	}

	s, ok := value.(string)
	if !ok {
		return debug.Err(fmt.Sprintf("idx.remove of %T", value), errNotString)
	}

	var k = []byte(s)
	val, err := idx.Get(k)
	if val == nil || err != nil {
		return err
	}

	var keys = unmarshalKeys(val)
	keys = keys.remove(id)

	if len(keys) == 0 {
		return idx.Delete(k)
	}

	return idx.Set(k, keys.marshal())
}

func (c *collectionIdx) clean(tx Tx, id string, data map[string]interface{}) error {
	for name, value := range data {
		var _, ok = value.(string)
		if !ok {
			continue
		}

		var idxName = "idx_" + c.name + "_" + name
		idx, err := tx.Bucket(idxName, false)
		if err != nil {
			return err
		}
		if idx == nil {
			continue
		}

		err = c.remove(idx, id, name, value, data)
		if err != nil {
			return err
		}
	}
	return nil
}

type keys []string

var emptyKeys = keys{}

func (a keys) marshal() []byte {
	sort.Strings(a)
	var buf = bytes.NewBuffer([]byte{})
	for _, k := range a {
		buf.Write([]byte(k))
		buf.WriteByte(0)
	}
	return buf.Bytes()
}

func unmarshalKeys(b []byte) keys {
	var list keys
	var start = 0
	for i := 0; i < len(b); i++ {
		if b[i] == 0 {
			list = append(list, string(b[start:i]))
			start = i + 1
		}
	}
	return list
}

func (a keys) remove(s string) keys {
	var i = indexOfString(a, s)
	if i >= 0 {
		return append(a[:i], a[i+1:]...)
	}
	return a
}

func indexOfString(a []string, s string) int {
	for i, v := range a {
		if v == s {
			return i
		}
	}
	return -1
}
