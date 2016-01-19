package kv

// Cursor defines interface of cursor in KV store.
type Cursor interface {
	First() ([]byte, []byte)
	Next() ([]byte, []byte)
	Seek(k []byte) ([]byte, []byte)
}

// Iter defines interface for iterators.
type Iter interface {
	Key() []byte
	Value() []byte
	Next() (bool, error)
}

// Store defines interface for KV stores.
type Store interface {
	Begin(writable bool) (Tx, error)
	Close() error
}

// Tx defines interface for transactions.
type Tx interface {
	Commit() error
	Rollback() error
	Bucket(name string, createIfNotExists bool) (Bucket, error)
}

// Bucket defines interface of KV bucket.
type Bucket interface {
	Get(k []byte) ([]byte, error)
	Set(k []byte, v []byte) error
	Delete(k []byte) error
	NextSequence() (string, error)
	Cursor() Cursor
}
