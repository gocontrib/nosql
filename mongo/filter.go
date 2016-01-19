package mongo

import (
	"github.com/gocontrib/nosql/q"
	"gopkg.in/mgo.v2/bson"
)

func mongoFilter(filter []interface{}) bson.M {
	if len(filter) == 0 {
		return nil
	}
	if len(filter) == 1 {
		return mongoCondition(filter[0])
	}
	var conds []bson.M
	for _, v := range filter {
		conds = append(conds, mongoCondition(v))
	}
	return bson.M{"$and": conds}
}

func mongoCondition(c interface{}) bson.M {
	switch t := c.(type) {
	case q.Not:
		return bson.M{"$not": t.Condition}
	case q.And:
		var conds []bson.M
		for _, v := range t {
			conds = append(conds, mongoCondition(v))
		}
		return bson.M{"$and": conds}
	case q.Or:
		var conds []bson.M
		for _, v := range t {
			conds = append(conds, mongoCondition(v))
		}
		return bson.M{"$or": conds}
	case q.M:
		var m = bson.M{}
		for field, value := range t {
			var key = field
			if field == "id" {
				key = "_id"
			}
			m[key] = mongoOp(value)
		}
		return m
	default:
		panic("invalid query")
	}
}

func mongoOp(v interface{}) interface{} {
	switch t := v.(type) {
	case q.Op:
		var op = "$" + string(t.Kind)
		return bson.M{op: t.Value}
	case q.In:
		return bson.M{"$in": []interface{}(t)}
	case q.NotIn:
		return bson.M{"$nin": []interface{}(t)}
	default:
		return v
	}
}
