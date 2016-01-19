package kv

import (
	"errors"
	"reflect"
	"sync"

	"github.com/gocontrib/nosql"
)

var (
	errNotFound     = errors.New("not found")
	errNotSliceAddr = errors.New("result argument must be a slice address")
	errNotString    = errors.New("value must have string type")
)

// New data store based on KV store.
func New(db Store) data.Store {
	if verbose {
		db = &debugStore{db}
	}
	return &store{
		db: db,
	}
}

type store struct {
	sync.Mutex
	db      Store
	idxmeta map[reflect.Type][]idxmeta
}

// Collection returns collection by name.
func (s *store) Collection(name string) data.Collection {
	var tx, err = s.db.Begin(true)
	if err != nil {
		debug.Err("db.Begin", err)
		panic(err)
	}

	_, err = tx.Bucket(name, true)
	if err != nil {
		debug.Err("tx.Bucket", err)
		tx.Rollback()
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		debug.Err("tx.Commit", err)
		tx.Rollback()
		panic(err)
	}

	return newCollection(s, name)
}

// Close performs cleanups.
func (s *store) Close() error {
	return s.db.Close()
}
