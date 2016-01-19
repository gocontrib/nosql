package postgresql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gocontrib/nosql/q"
)

func makeFilter(filter []interface{}) (string, []interface{}) {
	var q = &filterBuilder{}
	var s = q.build(filter)
	return s, q.params
}

type filterBuilder struct {
	params []interface{}
}

func (b *filterBuilder) build(filter []interface{}) string {
	var conds []string
	for _, v := range filter {
		conds = append(conds, b.condition(v))
	}
	return strings.Join(conds, " and ")
}

func (b *filterBuilder) condition(c interface{}) string {
	switch t := c.(type) {
	case q.Not:
		return fmt.Sprintf("not(%s)", b.condition(t.Condition))
	case q.And:
		var conds []string
		for _, v := range t {
			var cond = b.condition(v)
			if len(cond) == 0 {
				continue
			}
			conds = append(conds, cond)
		}
		return strings.Join(conds, " and ")
	case q.Or:
		var conds []string
		for _, v := range t {
			var cond = b.condition(v)
			if len(cond) == 0 {
				continue
			}
			conds = append(conds, cond)
		}
		return strings.Join(conds, " or ")
	case q.M:
		var conds []string
		for k, v := range t {
			var cond = b.field(k, v)
			if len(cond) == 0 {
				continue
			}
			conds = append(conds, cond)
		}
		return strings.Join(conds, " and ")
	default:
		panic("invalid query")
	}
}

func (b *filterBuilder) field(name string, value interface{}) string {
	if name == "id" || name == "_id" {
		var val = b.mapInt(value)
		if val == nil {
			return ""
		}
		value = val
	}
	var field = pgMapField(name)
	switch t := value.(type) {
	case q.In:
		var values []string
		for _, v := range t {
			values = append(values, b.param(v))
		}
		return fmt.Sprintf("%s IN (%s)", field, strings.Join(values, ","))
	case q.NotIn:
		var values []string
		for _, v := range t {
			values = append(values, b.param(v))
		}
		return fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(values, ","))
	case q.Op:
		return fmt.Sprintf("%s %s %s", field, sqlop(t.Kind), b.param(t.Value))
	default:
		return fmt.Sprintf("%s = %s", field, b.param(value))
	}
}

func pgMapField(name string) string {
	if name == "id" || name == "_id" {
		return "id"
	}
	return fmt.Sprintf("data->>'%s'", name)
}

func (b *filterBuilder) mapInt(value interface{}) interface{} {
	switch t := value.(type) {
	case q.In:
		var values = q.In{}
		for _, v := range t {
			var val = parseInt(v)
			if val == nil {
				return nil
			}
			values = append(values, val)
		}
		return values
	case q.NotIn:
		var values = q.NotIn{}
		for _, v := range t {
			var val = parseInt(v)
			if val == nil {
				return nil
			}
			values = append(values, val)
		}
		return values
	case q.Op:
		var val = parseInt(value)
		if val == nil {
			return nil
		}
		return q.Op{t.Kind, val}
	default:
		var val = parseInt(value)
		if val == nil {
			return nil
		}
		return val
	}
}

func parseInt(val interface{}) interface{} {
	if _, ok := val.(int64); ok {
		return val
	}
	if _, ok := val.(uint64); ok {
		return val
	}
	s, ok := val.(string)
	if ok {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil
		}
		return i
	}
	return nil
}

func (b *filterBuilder) param(val interface{}) string {
	b.params = append(b.params, val)
	return fmt.Sprintf("%s%d", "$", len(b.params))
}

// returns equvivalent SQL operator
func sqlop(v q.OpKind) string {
	switch v {
	case q.OpLT:
		return "<"
	case q.OpLTE:
		return "<="
	case q.OpGT:
		return ">"
	case q.OpGTE:
		return ">="
	case q.OpNE:
		return "<>"
	default:
		return ""
	}
}
