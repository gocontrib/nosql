package kv

import (
	"github.com/gocontrib/nosql/q"
	"github.com/gocontrib/nosql/util"
)

// FilterFn function to filter key-value pairs.
type FilterFn func(k string, v map[string]interface{}) bool

// MakeFilterFn creates filter function for given filter.
func MakeFilterFn(filter []interface{}) FilterFn {
	if len(filter) == 0 {
		return nil
	}
	var conds []FilterFn
	for _, i := range filter {
		conds = append(conds, condition(i))
	}
	return and(conds)
}

func condition(c interface{}) FilterFn {
	switch t := c.(type) {
	case q.Not:
		return not(condition(t.Condition))
	case q.And:
		var conds []FilterFn
		for _, i := range t {
			conds = append(conds, condition(i))
		}
		if len(conds) == 1 {
			return conds[0]
		}
		return and(conds)
	case q.Or:
		var conds []FilterFn
		for _, i := range t {
			conds = append(conds, condition(i))
		}
		if len(conds) == 1 {
			return conds[0]
		}
		return or(conds)
	case q.M:
		if len(t) == 0 {
			panic("invalid query")
		}
		var conds []FilterFn
		for k, v := range t {
			conds = append(conds, fieldFilter(k, v))
		}
		if len(conds) == 1 {
			return conds[0]
		}
		return and(conds)
	}
	return nil
}

func fieldFilter(name string, value interface{}) FilterFn {
	if name == "_id" {
		name = "id"
	}
	switch t := value.(type) {
	case q.In:
		return field(name, func(v interface{}) bool {
			for _, i := range t {
				if eq(v, i) {
					return true
				}
			}
			return false
		})
	case q.NotIn:
		return field(name, func(v interface{}) bool {
			for _, i := range t {
				if eq(v, i) {
					return false
				}
			}
			return true
		})
	case q.Op:
		var val = t.Value
		switch t.Kind {
		case q.OpLT:
			return field(name, func(v interface{}) bool {
				return lt(v, val)
			})
		case q.OpLTE:
			return field(name, func(v interface{}) bool {
				return lte(v, val)
			})
		case q.OpGT:
			return field(name, func(v interface{}) bool {
				return gt(v, val)
			})
		case q.OpGTE:
			return field(name, func(v interface{}) bool {
				return gte(v, val)
			})
		case q.OpNE:
			return field(name, func(v interface{}) bool {
				return !eq(v, val)
			})
		default:
			panic("invalid op")
		}
	default:
		return field(name, func(v interface{}) bool {
			return eq(v, value)
		})
	}
	return nil
}

func field(name string, p func(interface{}) bool) FilterFn {
	return func(k string, v map[string]interface{}) bool {
		var e, ok = v[name]
		if !ok {
			return false
		}
		return p(e)
	}
}

func not(f FilterFn) FilterFn {
	return func(k string, v map[string]interface{}) bool {
		return !f(k, v)
	}
}

func and(conds []FilterFn) FilterFn {
	return func(k string, v map[string]interface{}) bool {
		for _, c := range conds {
			if !c(k, v) {
				return false
			}
		}
		return true
	}
}

func or(conds []FilterFn) FilterFn {
	return func(k string, v map[string]interface{}) bool {
		for _, c := range conds {
			if c(k, v) {
				return true
			}
		}
		return false
	}
}

func eq(a, b interface{}) bool {
	return util.Compare(a, b) == 0
}

func lt(a, b interface{}) bool {
	return util.Compare(a, b) < 0
}

func lte(a, b interface{}) bool {
	return util.Compare(a, b) <= 0
}

func gt(a, b interface{}) bool {
	return util.Compare(a, b) > 0
}

func gte(a, b interface{}) bool {
	return util.Compare(a, b) >= 0
}
