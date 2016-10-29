package redis

import (
	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/kv"
)

var debug = log.IfDebug("redis")

func init() {
	data.RegisterDriver(&driver{}, "redis")
}

type driver struct{}

func (d *driver) Open(url, db string) (data.Store, error) {
	// TODO support db parameter if it is not in URL
	return Open(url, false)
}

// Open redis store.
func Open(url string, dropDatabase bool) (data.Store, error) {
	db, err := newRedisStore(url, dropDatabase)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

// New redis-like store.
func New(backend Store) data.Store {
	return kv.New(&store{backend})
}

type store struct {
	db Store
}

func (s *store) Begin(writable bool) (kv.Tx, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	return &kvtx{tx}, nil
}

func (s *store) Close() error {
	s.db.Close()
	return nil
}

type kvtx struct {
	tx Tx
}

func (t *kvtx) Commit() error {
	return t.tx.Commit()
}

func (t *kvtx) Rollback() error {
	return t.tx.Rollback()
}

func (t *kvtx) Bucket(name string, createIfNotExists bool) (kv.Bucket, error) {
	var prefix = name + separator
	return &bucket{
		prefix: prefix,
		keyID:  []byte("_" + prefix + keyID),
		tx:     t.tx,
	}, nil
}
