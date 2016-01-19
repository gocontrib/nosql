package kv

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/gocontrib/nosql/util"
)

// SortIter creates sortable iterator.
func SortIter(iter Iter, sort []string) Iter {
	if len(sort) == 0 {
		return iter
	}
	return &sortIter{
		iter: iter,
		sort: sort,
	}
}

type pair struct {
	key   []byte
	value []byte
	data  map[string]interface{}
}

type sortIter struct {
	iter        Iter
	sort        []string
	initialized bool
	closed      bool
	data        []*pair
	idx         int
}

func (c *sortIter) Key() []byte {
	return c.data[c.idx].key
}

func (c *sortIter) Value() []byte {
	return c.data[c.idx].value
}

func (c *sortIter) Next() (bool, error) {
	if c.closed {
		return false, nil
	}
	if !c.initialized {
		c.initialized = true
		for {
			ok, err := c.iter.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			var p = &pair{
				key:   c.iter.Key(),
				value: c.iter.Value(),
				data:  make(map[string]interface{}),
			}
			err = json.Unmarshal(p.value, &p.data)
			if err != nil {
				panic(err)
			}
			c.data = append(c.data, p)
			sort.Sort(c)
		}
	} else {
		c.idx++
	}
	if c.idx >= len(c.data) {
		c.closed = true
		return false, nil
	}
	return true, nil
}

// sort.Interface
func (c *sortIter) Len() int {
	return len(c.data)
}

func (c *sortIter) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

func (c *sortIter) Less(i, j int) bool {
	var a = c.data[i]
	var b = c.data[j]
	for _, k := range c.sort {
		var asc = true
		if strings.HasPrefix(k, "-") {
			k = k[1:]
			asc = false
		}
		var v1, ok1 = a.data[k]
		if !ok1 {
			v1 = nil
		}
		var v2, ok2 = b.data[k]
		if !ok2 {
			v2 = nil
		}
		var t = util.Compare(v1, v2)
		if t == 0 {
			continue
		}
		if t < 0 {
			return asc
		}
		return !asc
	}
	return false
}
