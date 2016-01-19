package mongo

import (
	"time"

	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/reflection"
	"gopkg.in/mgo.v2/bson"
)

type collection struct {
	store *store
	name  string
}

// Name of collection.
func (c *collection) Name() string {
	return c.name
}

// Count returns number of documents in the collection.
func (c *collection) Count() (int64, error) {
	var session = c.store.session.Copy()
	defer session.Close()
	var db = session.DB(c.store.dbname)
	var collection = db.C(c.name)
	var n, err = collection.Count()
	return int64(n), err
}

// Insert given documents to the collection.
func (c *collection) Insert(docs ...interface{}) error {
	var now = time.Now().UTC()
	for _, doc := range docs {
		var meta = reflection.GetMeta(doc)
		meta.SetID(doc, bson.NewObjectId().Hex())
		meta.SetCreatedAt(doc, now)
		meta.SetUpdatedAt(doc, now)
	}
	var session = c.store.session.Copy()
	defer session.Close()
	var db = session.DB(c.store.dbname)
	var collection = db.C(c.name)
	return collection.Insert(docs...)
}

// Gets one result by id.
func (c *collection) Get(id string, result interface{}) error {
	var session = c.store.session.Copy()
	var db = session.DB(c.store.dbname)
	var collection = db.C(c.name)
	return collection.FindId(id).One(result)
}

// Gets all results.
func (c *collection) GetAll(result interface{}) error {
	return c.Find().All(result)
}

// Find opens new query session.
func (c *collection) Find(filter ...interface{}) data.Result {
	return &view{collection: c, filter: filter}
}

// Update given document.
func (c *collection) Update(selector interface{}, doc interface{}) error {
	// update meta
	var meta = reflection.GetMeta(doc)
	meta.SetUpdatedAt(doc, time.Now().UTC())
	// commit to data store
	var session = c.store.session.Copy()
	defer session.Close()
	var db = session.DB(c.store.dbname)
	var collection = db.C(c.name)
	var id, ok = selector.(string)
	if ok {
		return collection.UpdateId(id, doc)
	} else {
		return collection.Update(mongoFilter([]interface{}{selector}), doc)
	}
}

// Delete documents that match given filter.
func (c *collection) Delete(selector interface{}) error {
	var session = c.store.session.Copy()
	defer session.Close()
	var db = session.DB(c.store.dbname)
	var collection = db.C(c.name)
	if id, ok := selector.(string); ok {
		return collection.RemoveId(id)
	}
	var err error
	if selector == nil {
		_, err = collection.RemoveAll(nil)
	} else {
		_, err = collection.RemoveAll(mongoFilter([]interface{}{selector}))
	}
	return err
}
