package postgresql

import (
	"fmt"
	"strings"

	"github.com/gocontrib/nosql"
)

type query struct {
	collection *collection
	table      string
	filter     []interface{}
	sort       []string
	limit      int64
	skip       int64
}

func (q *query) copy() *query {
	return &query{
		collection: q.collection,
		table:      q.table,
		filter:     q.filter,
		sort:       q.sort,
		limit:      q.limit,
		skip:       q.skip,
	}
}

// makes select statement with parameters
func (q *query) makeSelectStmt(cols string) (string, []interface{}) {
	if len(cols) == 0 {
		cols = "id, data"
	}
	var limit = ""
	if q.limit > 0 {
		limit = fmt.Sprintf(" LIMIT %d", q.limit)
	}
	var offset = ""
	if q.skip > 0 {
		offset = fmt.Sprintf(" OFFSET %d", q.skip)
	}
	var orderby = q.orderBy()
	var filter, args = makeFilter(q.filter)
	var where = ""
	if len(filter) > 0 {
		where = fmt.Sprintf(" WHERE %s", filter)
	}
	var query = fmt.Sprintf("SELECT %s FROM %s%s%s%s%s", cols, q.table, limit, offset, orderby, where)
	return query, args
}

func (q *query) orderBy() string {
	if len(q.sort) == 0 {
		return ""
	}
	var list []string
	for _, f := range q.sort {
		if strings.HasPrefix(f, "-") {
			list = append(list, fmt.Sprintf("%s DESC", pgMapField(f[1:])))
		} else {
			list = append(list, pgMapField(f))
		}
	}
	return fmt.Sprintf(" ORDER BY %s", strings.Join(list, ", "))
}

// Count returns the number of items that match the set conditions.
func (q *query) Count() (int64, error) {
	return q.collection.QueryCount(q)
}

// One fetches the first result within the result set.
func (q *query) One(result interface{}) error {
	return q.collection.FindOne(result, q)
}

// All fetches all results within the result set.
func (q *query) All(result interface{}) error {
	return q.collection.FindAll(result, q)
}

// Limit defines the maximum number of results in this set.
func (q *query) Limit(n int64) data.Result {
	var q2 = q.copy()
	q2.limit = n
	return q2
}

// Skip ignores first *n* results.
func (q *query) Skip(n int64) data.Result {
	var q2 = q.copy()
	q2.skip = n
	return q2
}

// Sort results by given fields.
func (q *query) Sort(fields ...string) data.Result {
	var q2 = q.copy()
	q2.sort = fields
	return q2
}

// Cursor executes query and returns cursor capable of going over all the results.
func (q *query) Cursor() (data.Cursor, error) {
	var stmt, args = q.makeSelectStmt("")
	var rows, err = q.collection.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	return &cursor{q.collection.store, rows}, nil
}
