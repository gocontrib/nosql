package mongo

import (
	"github.com/gocontrib/nosql"
	"gopkg.in/mgo.v2"
)

type view struct {
	collection *collection
	filter     []interface{}
	limit      int
	skip       int
	sort       []string
}

func (r *view) copy() *view {
	return &view{
		collection: r.collection,
		filter:     r.filter,
		limit:      r.limit,
		skip:       r.skip,
		sort:       r.sort,
	}
}

func (r *view) session() *mgo.Session {
	return r.collection.store.session.Copy()
}

func (r *view) query(session *mgo.Session) *mgo.Query {
	var db = session.DB(r.collection.store.dbname)
	var collection = db.C(r.collection.name)
	var query = collection.Find(mongoFilter(r.filter))
	if r.skip > 0 {
		query = query.Skip(r.skip)
	}
	if r.limit > 0 {
		query = query.Limit(r.limit)
	}
	if len(r.sort) > 0 {
		query = query.Sort(r.sort...)
	}
	return query
}

// Count returns the number of items that match the set conditions.
func (r *view) Count() (int64, error) {
	var s = r.session()
	defer s.Close()
	var query = r.query(s)
	var n, err = query.Count()
	return int64(n), err
}

// One fetches the first result within the result set.
func (r *view) One(result interface{}) error {
	var s = r.session()
	defer s.Close()
	return r.query(s).One(result)
}

// All fetches all results within the result set.
func (r *view) All(result interface{}) error {
	var s = r.session()
	defer s.Close()
	return r.query(s).All(result)
}

// Limit defines the maximum number of results in this set.
func (r *view) Limit(n int64) data.Result {
	var t = r.copy()
	t.limit = int(n)
	return t
}

// Skip ignores first *n* results.
func (r *view) Skip(n int64) data.Result {
	var t = r.copy()
	t.skip = int(n)
	return t
}

// Sort results by given fields.
func (r *view) Sort(fields ...string) data.Result {
	if len(fields) == 0 {
		return r
	}
	var t = r.copy()
	t.sort = fields
	return t
}

// Cursor executes query and returns cursor capable of going over all the results.
func (r *view) Cursor() (data.Cursor, error) {
	var s = r.session()
	var iter = r.query(s).Iter()
	var err = iter.Err()
	if err != nil {
		return nil, err
	}
	return &cursor{iter}, nil
}
