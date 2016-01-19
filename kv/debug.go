package kv

import (
	"github.com/gocontrib/log"
)

var (
	debug   = log.IfDebug("kv")
	verbose = false
)

type debugStore struct {
	db Store
}

func (t *debugStore) Begin(writable bool) (Tx, error) {
	debug.Info("db.Begin(%v)", writable)
	tx, err := t.db.Begin(writable)
	if err != nil {
		debug.Error("Begin failed: %v", err)
		return nil, err
	}
	return &debugTx{tx}, nil
}

func (t *debugStore) Close() error {
	debug.Info("db.Close()")
	var err = t.db.Close()
	if err != nil {
		debug.Error("Close failed: %v", err)
	}
	return err
}

type debugTx struct {
	tx Tx
}

func (t *debugTx) Commit() error {
	debug.Info("tx.Commit()")
	var err = t.tx.Commit()
	if err != nil {
		debug.Error("Commit failed: %v", err)
		return err
	}
	return nil
}

func (t *debugTx) Rollback() error {
	debug.Info("tx.Rollback()")
	var err = t.tx.Rollback()
	if err != nil {
		debug.Error("Rollback failed: %v", err)
		return err
	}
	return nil
}

func (t *debugTx) Bucket(name string, createIfNotExists bool) (Bucket, error) {
	debug.Info("tx.Bucket(%s, %v)", name, createIfNotExists)
	b, err := t.tx.Bucket(name, createIfNotExists)
	if err != nil {
		debug.Error("Bucket failed: %v", err)
		return nil, err
	}
	if b == nil {
		debug.Info("Bucket not found: %s", name)
		return nil, nil
	}
	return &debugBucket{name, b}, nil
}

type debugBucket struct {
	name   string
	bucket Bucket
}

func (t *debugBucket) Get(k []byte) ([]byte, error) {
	debug.Info("%s.Get(%s)", t.name, k)
	v, err := t.bucket.Get(k)
	if err != nil {
		debug.Error("Get failed: %v", err)
		return nil, err
	}
	return v, nil
}

func (t *debugBucket) Set(k []byte, v []byte) error {
	debug.Info("%s.Set(%s)", t.name, k)
	err := t.bucket.Set(k, v)
	if err != nil {
		debug.Error("Set failed: %v", err)
		return err
	}
	return nil
}

func (t *debugBucket) Delete(k []byte) error {
	debug.Info("%s.Delete(%s)", t.name, k)
	err := t.bucket.Delete(k)
	if err != nil {
		debug.Error("Delete failed: %v", err)
		return err
	}
	return nil
}

func (t *debugBucket) NextSequence() (string, error) {
	debug.Info("%s.NextSequence()", t.name)
	s, err := t.bucket.NextSequence()
	if err != nil {
		debug.Error("NextSequence failed: %v", err)
		return "", err
	}
	return s, nil
}

func (t *debugBucket) Cursor() Cursor {
	debug.Info("%s.Cursor()", t.name)
	return t.bucket.Cursor()
}
