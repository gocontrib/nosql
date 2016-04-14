package postgresql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/q"
	"github.com/gocontrib/nosql/reflection"
)

type collection struct {
	sync.Mutex
	store   *store
	db      *sql.DB
	name    string
	created bool
}

func (c *collection) init() error {
	c.Lock()
	defer c.Unlock()
	if c.created {
		return nil
	}
	const schema = "(id SERIAL PRIMARY KEY, data jsonb)"
	var _, err = c.db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", c.name, schema))
	if err != nil {
		return err
	}
	c.created = true
	_, err = c.db.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s on %s using GIN(data jsonb_path_ops)", c.name, c.name))
	return err
}

func (c *collection) Exec(query string, args ...interface{}) (sql.Result, error) {
	var err = c.init()
	if err != nil {
		return nil, err
	}
	if debug.Enabled() {
		debug.Debug("%s; args: %v", query, args)
	}
	return c.db.Exec(query, args...)
}

func (c *collection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var err = c.init()
	if err != nil {
		return nil, err
	}
	if debug.Enabled() {
		debug.Debug("%s; args: %v", query, args)
	}
	return c.db.Query(query, args...)
}

func (c *collection) QueryRow(query string, args ...interface{}) (*sql.Row, error) {
	var err = c.init()
	if err != nil {
		return nil, err
	}
	if debug.Enabled() {
		debug.Debug("%s; args: %v", query, args)
	}
	return c.db.QueryRow(query, args...), nil
}

// Name of collection.
func (c *collection) Name() string {
	return c.name
}

func (c *collection) QueryCount(query *query) (int64, error) {
	var stmt, args = query.makeSelectStmt("count(*) as count")
	var row, err = c.QueryRow(stmt, args...)
	if err != nil {
		return 0, err
	}
	var count int64
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Count returns number of documents in the collection.
func (c *collection) Count() (int64, error) {
	return c.Find().Count()
}

// Insert given documents to the collection.
func (c *collection) Insert(docs ...interface{}) error {
	// TODO insert multiple docs in one transaction
	for _, d := range docs {
		var err = c.insertOne(d)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *collection) insertOne(doc interface{}) error {
	var now = time.Now().UTC()
	var meta = reflection.GetMeta(doc)
	meta.SetCreatedAt(doc, now)
	meta.SetUpdatedAt(doc, now)
	var b, err = json.Marshal(doc)
	if err != nil {
		return err
	}
	var cmd = fmt.Sprintf("INSERT INTO %s (data) VALUES ('%s') RETURNING id", c.name, string(b))
	row, err := c.QueryRow(cmd)
	if err != nil {
		return err
	}
	var id int64
	err = row.Scan(&id)
	if err != nil {
		return err
	}
	var sid = strconv.FormatInt(id, 10)
	meta.SetID(doc, sid)
	return nil
}

// Finds one result.
func (c *collection) FindOne(result interface{}, query *query) error {
	var stmt, args = query.makeSelectStmt("")
	var row, err = c.QueryRow(stmt, args...)
	if err != nil {
		return err
	}
	var id int64
	var data []byte
	err = row.Scan(&id, &data)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, result)
	if err != nil {
		return err
	}
	var meta = reflection.GetMeta(result)
	var sid = strconv.FormatInt(id, 10)
	meta.SetID(result, sid)
	return nil
}

// Finds all results.
func (c *collection) FindAll(result interface{}, query *query) error {
	rval := reflect.ValueOf(result)
	if rval.Kind() != reflect.Ptr || rval.Elem().Kind() != reflect.Slice {
		return errors.New("result argument must be a slice address")
	}

	var stmt, args = query.makeSelectStmt("")
	var rows, err = c.Query(stmt, args...)
	if err != nil {
		return err
	}

	slice := rval.Elem()
	slice = slice.Slice(0, slice.Cap())
	elemType := slice.Type().Elem()

	var i = 0
	var meta = reflection.GetMeta(elemType)

	for ; rows.Next(); i++ {

		var id int64
		var data []byte
		err = rows.Scan(&id, &data)
		if err != nil {
			return err
		}

		var isnew = false
		var item interface{}
		if slice.Len() == i {
			isnew = true
			elemp := reflect.New(elemType)
			item = elemp.Interface()
		} else {
			item = slice.Index(i).Addr().Interface()
		}

		err = json.Unmarshal(data, item)
		if err != nil {
			return err
		}

		var sid = strconv.FormatInt(id, 10)
		meta.SetID(item, sid)

		if isnew {
			slice = reflect.Append(slice, reflect.ValueOf(item).Elem())
			slice = slice.Slice(0, slice.Cap())
		}
	}

	rval.Elem().Set(slice.Slice(0, i))

	return nil
}

// Gets one result by id.
func (c *collection) Get(id string, result interface{}) error {
	return c.Find(q.M{"id": id}).One(result)
}

// Gets all results.
func (c *collection) GetAll(result interface{}) error {
	return c.Find().All(result)
}

// Find opens new query session.
func (c *collection) Find(filter ...interface{}) data.Result {
	return &query{
		collection: c,
		table:      c.name,
		filter:     filter,
	}
}

// Update given document.
func (c *collection) Update(selector interface{}, doc interface{}) error {
	var b, err = json.Marshal(doc)
	if err != nil {
		return err
	}
	// update meta
	var meta = reflection.GetMeta(doc)
	meta.SetUpdatedAt(doc, time.Now().UTC())
	// commit to data store
	var json = string(b)
	if id := parseInt(selector); id != nil {
		_, err = c.Exec(fmt.Sprintf("UPDATE %s SET data=$1 WHERE id=$2", c.name), json, id)
		return err
	}
	var filter, args = makeFilter([]interface{}{selector})
	args = append([]interface{}{json}, args...)
	_, err = c.Exec(fmt.Sprintf("UPDATE %s SET data=$1 WHERE %s", c.name, filter), args...)
	return err
}

// Delete documents that match given filter.
func (c *collection) Delete(selector interface{}) error {
	if id := parseInt(selector); id != nil {
		var _, err = c.Exec(fmt.Sprintf("DELETE FROM %s WHERE id=$1", c.name), id)
		return err
	}
	var (
		args  []interface{}
		query string
		cond  string
	)
	if selector == nil {
		query = fmt.Sprintf("DELETE FROM %s", c.name)
	} else {
		cond, args = makeFilter([]interface{}{selector})
		query = fmt.Sprintf("DELETE FROM %s WHERE %s", c.name, cond)
	}
	_, err := c.Exec(query, args...)
	return err
}
