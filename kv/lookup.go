package kv

import (
	"sort"

	"github.com/gocontrib/nosql/q"
)

type lookup struct {
	collection *collection
	tx         Tx
}

func (c lookup) find(f []interface{}) keys {
	return c.and(f)
}

func (c lookup) condition(f interface{}) keys {
	switch t := f.(type) {
	case q.Not:
		return emptyKeys
	case q.And:
		return c.and(t)
	case q.Or:
		return c.or(t)
	case q.M:
		var set hashset
		for name, val := range t {
			var keys = c.field(name, val)
			if set == nil {
				set = newHashset(keys)
				continue
			}
			var set2 = make(hashset)
			for _, k := range keys {
				if set.has(k) {
					set2.add(k)
				}
			}
			set = set2
			if len(set) == 0 {
				return emptyKeys
			}
		}
		if set == nil {
			return emptyKeys
		}
		return set.toArray()
	}
	return emptyKeys
}

func (c lookup) field(name string, value interface{}) keys {
	if name == "id" || name == "_id" {
		var s, ok = value.(string)
		if !ok {
			return emptyKeys
		}
		return keys{s}
	}

	var idxName = "idx_" + c.collection.name + "_" + name
	idx, err := c.tx.Bucket(idxName, false)
	if err != nil || idx == nil {
		return emptyKeys
	}

	switch v := value.(type) {
	case string:
		raw, err := idx.Get([]byte(v))
		if err != nil || raw == nil {
			return emptyKeys
		}
		return unmarshalKeys(raw)
	}

	return emptyKeys
}

func (c lookup) and(f []interface{}) keys {
	if len(f) == 1 {
		return c.condition(f[0])
	}
	var set = newHashset(c.condition(f[0]))
	if len(set) == 0 {
		return emptyKeys
	}
	for i := 1; i < len(f); i++ {
		var keys = c.condition(f[i])
		var set2 = make(hashset)
		for _, k := range keys {
			if set.has(k) {
				set2.add(k)
			}
		}
		set = set2
		if len(set) == 0 {
			return emptyKeys
		}
	}
	return set.toArray()
}

func (c lookup) or(f []interface{}) keys {
	if len(f) == 1 {
		return c.condition(f[0])
	}
	var set = newHashset(c.condition(f[0]))
	for i := 1; i < len(f); i++ {
		var keys = c.condition(f[i])
		for _, k := range keys {
			set.add(k)
		}
	}
	return set.toArray()
}

// determines whether given filter(s) can use index tables
func (c lookup) isSuitable(f interface{}) bool {
	switch t := f.(type) {
	case q.Not:
		return false
	case q.And:
		for _, i := range t {
			if !c.isSuitable(i) {
				return false
			}
		}
		return true
	case q.Or:
		for _, i := range t {
			if !c.isSuitable(i) {
				return false
			}
		}
		return true
	case []interface{}:
		for _, i := range t {
			if !c.isSuitable(i) {
				return false
			}
		}
		return true
	case q.M:
		for name, v := range t {
			switch v.(type) {
			case q.In:
				return false
			case q.NotIn:
				return false
			case q.Op:
				return false
			}
			// now only strings are indexed
			s, ok := v.(string)
			if !ok {
				return len(s) > 0
			}
			if name == "id" || name == "_id" {
				return true
			}
			var idxName = "idx_" + c.collection.name + "_" + name
			idx, err := c.tx.Bucket(idxName, false)
			if err != nil || idx == nil {
				return false
			}
		}
		return true
	}
	return false
}

// hashset of strings

type hashset map[string]struct{}

func (s hashset) has(k string) bool {
	_, h := s[k]
	return h
}

func (s hashset) add(k string) bool {
	if _, h := s[k]; h {
		return false
	}
	s[k] = struct{}{}
	return true
}

func (s hashset) toArray() []string {
	var a []string
	for k := range s {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

func newHashset(keys []string) hashset {
	var t = make(hashset)
	for _, k := range keys {
		t.add(k)
	}
	return t
}
