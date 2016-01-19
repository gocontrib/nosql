package redis

// Store defines interface for redis-like KV stores.
type Store interface {
	Begin() (Tx, error)
	Close() error
}

// Tx defines interface for transactions.
type Tx interface {
	Commit() error
	Rollback() error
	Get(k []byte) ([]byte, error)
	Set(k []byte, v []byte) error
	Delete(k []byte) error
	Exists(k []byte) (int64, error)
	GetInt64(k []byte) (int64, error)
	Incr(k []byte) (int64, error)
	Scan(prefix string, cursor int, count int, last []byte) (int, [][]byte, error)
}
