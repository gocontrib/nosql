package boltdb

import (
	"os"
	"path"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/kv"
)

func init() {
	data.RegisterDriver(&driver{}, "bolt", "boltdb")
}

type driver struct{}

func (d *driver) Open(dir, db string) (data.Store, error) {
	var dbpath = db
	if len(dir) > 0 {
		dbpath = path.Join(dir, db)
	}
	return Open(dbpath, false)
}

// Open bolt data store.
func Open(path string, dropDatabase bool) (data.Store, error) {
	if dropDatabase {
		if _, err := os.Stat(path); err == nil {
			os.Remove(path)
		}
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	// TODO init app buckets ()
	return kv.New(&store{db}), nil
}

// kv.Store impl

type store struct {
	db *bolt.DB
}

func (s *store) Begin(writable bool) (kv.Tx, error) {
	var tx, err = s.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &txImpl{
		tx:       tx,
		writable: writable,
	}, nil
}

// Close performs cleanups.
func (s *store) Close() error {
	return s.db.Close()
}

// kv.Tx impl

type txImpl struct {
	tx       *bolt.Tx
	writable bool
	closed   bool
}

func (t *txImpl) Commit() error {
	if t.closed {
		return nil
	}
	t.closed = true
	if t.writable {
		return t.tx.Commit()
	}
	return nil
}

func (t *txImpl) Rollback() error {
	if t.closed {
		return nil
	}
	t.closed = true
	return t.tx.Rollback()
}

func (t *txImpl) Bucket(name string, createIfNotExists bool) (kv.Bucket, error) {
	if createIfNotExists {
		b, err := t.tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return nil, err
		}
		return &bucketImpl{t.tx, b}, nil
	}
	b := t.tx.Bucket([]byte(name))
	if b == nil {
		return nil, nil
	}
	return &bucketImpl{t.tx, b}, nil
}

// kv.Bucket impl

type bucketImpl struct {
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

func (b *bucketImpl) Get(k []byte) ([]byte, error) {
	v := b.bucket.Get(k)
	return v, nil
}

func (b *bucketImpl) Set(k []byte, v []byte) error {
	return b.bucket.Put(k, v)
}

func (b *bucketImpl) Delete(k []byte) error {
	return b.bucket.Delete(k)
}

func (b *bucketImpl) NextSequence() (string, error) {
	i, err := b.bucket.NextSequence()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(int64(i), 10), nil
}

func (b *bucketImpl) Cursor() kv.Cursor {
	return b.bucket.Cursor()
}
