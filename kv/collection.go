package kv

import (
	"encoding/json"
	"time"

	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/reflection"
)

func newCollection(s *store, name string) *collection {
	return &collection{
		store: s,
		db:    s.db,
		name:  name,
		idx: &collectionIdx{
			store: s,
			name:  name,
		},
	}
}

type collection struct {
	store *store
	db    Store
	name  string
	idx   *collectionIdx
}

// Name of collection.
func (c *collection) Name() string {
	return c.name
}

// Count returns number of documents in the collection.
func (c *collection) Count() (int64, error) {
	var tx, err = c.db.Begin(false)
	if err != nil {
		return 0, err
	}

	defer tx.Rollback()

	bucket, err := tx.Bucket(c.name, false)
	if bucket == nil || err != nil {
		if err != nil {
			return 0, err
		}
		return 0, errNotFound
	}

	cursor := bucket.Cursor()
	var count int64
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		count++
	}

	return count, nil
}

// Insert given documents to the collection.
func (c *collection) Insert(docs ...interface{}) error {
	var tx, err = c.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	bucket, err := tx.Bucket(c.name, false)
	if bucket == nil || err != nil {
		if err != nil {
			return err
		}
		return errNotFound
	}

	var now = time.Now().UTC()

	for _, doc := range docs {
		id, err := bucket.NextSequence()
		if err != nil {
			return debug.Err("bucket.NextSequence", err)
		}

		var meta = reflection.GetMeta(doc)
		meta.SetID(doc, id)
		meta.SetCreatedAt(doc, now)
		meta.SetUpdatedAt(doc, now)

		json, err := marshal(doc)
		if err != nil {
			return err
		}

		err = bucket.Set([]byte(id), json)
		if err != nil {
			return err
		}

		err = c.idx.update(tx, id, doc, nil)
		if err != nil {
			return debug.Err("index.Update", err)
		}
	}

	return tx.Commit()
}

// Gets one result by id.
func (c *collection) Get(id string, result interface{}) error {
	var tx, err = c.db.Begin(false)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	bucket, err := tx.Bucket(c.name, false)
	if bucket == nil || err != nil {
		if err != nil {
			return err
		}
		return errNotFound
	}

	value, err := bucket.Get([]byte(id))
	if value == nil || err != nil {
		if err != nil {
			return err
		}
		return errNotFound
	}

	return unmarshal(value, result)
}

// Gets all results.
func (c *collection) GetAll(result interface{}) error {
	return c.Find().All(result)
}

// Find opens new query session.
func (c *collection) Find(filter ...interface{}) data.Result {
	return &view{
		collection: c,
		filter:     filter,
	}
}

// Update given document.
func (c *collection) Update(selector interface{}, doc interface{}) error {
	var now = time.Now().UTC()
	var meta = reflection.GetMeta(doc)
	meta.SetUpdatedAt(doc, now)

	var json, err = marshal(doc)
	if err != nil {
		return err
	}

	var id, ok = selector.(string)
	if ok {
		tx, err := c.db.Begin(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()

		bucket, err := tx.Bucket(c.name, false)
		if bucket == nil || err != nil {
			if err != nil {
				return err
			}
			return errNotFound
		}

		err = c.update(tx, bucket, doc, []byte(id), json)
		if err != nil {
			return err
		}

		return tx.Commit()
	}

	cursor, err := c.cursor(selector)
	if err != nil {
		return err
	}

	for cursor.next() {
		err = c.update(cursor.transaction(), cursor.bucket(), doc, cursor.key(), json)
		cursor.Close()
		return err
	}

	return errNotFound
}

func (c *collection) update(tx Tx, bucket Bucket, doc interface{}, k, v []byte) error {
	old, err := bucket.Get(k)
	if old == nil || err != nil {
		if err != nil {
			return err
		}
		return errNotFound
	}

	err = bucket.Set(k, v)
	if err != nil {
		return err
	}

	var data map[string]interface{}
	err = unmarshal(old, &data)
	if err != nil {
		return err
	}

	return c.idx.update(tx, string(k), doc, data)
}

// Delete documents that match given filter.
func (c *collection) Delete(selector interface{}) error {
	var id, ok = selector.(string)
	if ok {
		tx, err := c.db.Begin(true)
		if err != nil {
			return err
		}

		defer tx.Rollback()

		bucket, err := tx.Bucket(c.name, false)
		if bucket == nil || err != nil {
			if err != nil {
				return err
			}
			return errNotFound
		}

		err = c.delete(tx, bucket, []byte(id), nil)
		if err != nil {
			return err
		}

		return tx.Commit()
	}

	cursor, err := c.cursor(selector)
	if err != nil {
		return err
	}

	for cursor.next() {
		var k = cursor.key()
		err = c.delete(cursor.transaction(), cursor.bucket(), k, cursor.value())
		if err != nil {
			return err
		}
	}

	return cursor.Close()
}

func (c *collection) delete(tx Tx, bucket Bucket, k, v []byte) error {
	var err error
	if v == nil {
		v, err = bucket.Get(k)
		if err != nil {
			return err
		}
	}
	if v == nil {
		return errNotFound
	}

	var data map[string]interface{}
	err = unmarshal(v, &data)
	if err != nil {
		return err
	}

	err = bucket.Delete(k)
	if err != nil {
		return err
	}

	return c.idx.clean(tx, string(k), data)
}

func (c *collection) cursor(selector interface{}) (*cursor, error) {
	var filter []interface{}
	if selector != nil {
		filter = append(filter, selector)
	}
	var v = &view{
		collection: c,
		filter:     filter,
	}
	return v.cursor(true)
}

func marshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, debug.Err("json.Marshal", err)
	}
	return b, nil
}

func unmarshal(data []byte, result interface{}) error {
	return debug.Err("json.Unmarshal", json.Unmarshal(data, result))
}
