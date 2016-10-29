package ledis

import (
	"os"
	"strconv"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/redis"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
)

var debug = log.IfDebug("ledis")

func init() {
	data.RegisterDriver(&driver{}, "ledis", "ledisdb")
}

type driver struct{}

func (d *driver) Open(path, db string) (data.Store, error) {
	i, err := strconv.ParseInt(db, 10, 64)
	if err != nil {
		return nil, err
	}
	return Open(path, int(i), false)
}

// Open ledis store.
func Open(path string, db int, dropDatabase bool) (data.Store, error) {
	if dropDatabase {
		if _, err := os.Stat(path); err == nil {
			os.RemoveAll(path)
		}
	}

	cfg := config.NewConfigDefault()
	cfg.DataDir = path

	l, err := ledis.Open(cfg)
	if err != nil {
		return nil, debug.Err("open", err)
	}

	d, err := l.Select(db)
	if err != nil {
		return nil, debug.Err("select", err)
	}

	return redis.New(&store{l, d}), nil
}

type store struct {
	ledis *ledis.Ledis
	db    *ledis.DB
}

func (s *store) Begin() (redis.Tx, error) {
	return s, nil
}

func (s *store) Close() error {
	s.ledis.Close()
	return nil
}

func (s *store) Commit() error {
	return nil
}

func (s *store) Rollback() error {
	return nil
}

func (s *store) Get(k []byte) ([]byte, error) {
	return s.db.Get(k)
}

func (s *store) Set(k []byte, v []byte) error {
	return s.db.Set(k, v)
}

func (s *store) Delete(k []byte) error {
	_, err := s.db.Del(k)
	return err
}

func (s *store) Exists(k []byte) (int64, error) {
	return s.db.Exists(k)
}

func (s *store) GetInt64(k []byte) (int64, error) {
	return ledis.StrInt64(s.db.Get(k))
}

func (s *store) Incr(k []byte) (int64, error) {
	return s.db.Incr(k)
}

func (s *store) Scan(prefix string, cursor int, count int, last []byte) (int, [][]byte, error) {
	var inclusive = false
	if cursor == 0 {
		last = makeKey(prefix, 1)
		inclusive = true
	}

	var match = "^" + prefix
	keys, err := s.db.Scan(ledis.KV, last, count, inclusive, match)
	if err != nil {
		return 0, nil, debug.Err("scan", err)
	}

	var next = cursor + 1
	if len(keys) < count {
		next = 0
	}

	return next, keys, nil
}

func makeKey(prefix string, id int64) []byte {
	return []byte(prefix + strconv.FormatInt(id, 10))
}
