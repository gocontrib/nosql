package redis

import (
	"strconv"

	"github.com/gocontrib/nosql/kv"
)

type bucket struct {
	prefix string
	keyID  []byte
	tx     Tx
}

const (
	separator = "_"
	keyID     = "id"
)

func (b *bucket) LastID() (int64, error) {
	var k = b.keyID
	n, err := b.tx.Exists(k)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	return b.tx.GetInt64(k)
}

func (b *bucket) Get(k []byte) ([]byte, error) {
	return b.get(string(k))
}

func (b *bucket) get(key string) ([]byte, error) {
	var k = []byte(b.prefix + key)
	var val, err = b.tx.Get(k)
	if err != nil {
		return nil, debug.Err("get", err)
	}
	return val, err
}

func (b *bucket) Set(k []byte, v []byte) error {
	k = []byte(b.prefix + string(k))
	return debug.Err("set", b.tx.Set(k, v))
}

func (b *bucket) Delete(k []byte) error {
	k = []byte(b.prefix + string(k))
	return debug.Err("delete", b.tx.Delete(k))
}

func (b *bucket) NextSequence() (string, error) {
	n, err := b.tx.Incr(b.keyID)
	if err != nil {
		return "", debug.Err("increment", err)
	}
	return strconv.FormatInt(n, 10), nil
}

func (b *bucket) Cursor() kv.Cursor {
	return &cursor{bucket: b}
}
