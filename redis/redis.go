package redis

import (
	"github.com/garyburd/redigo/redis"
	"github.com/gocontrib/log"
	"github.com/soveran/redisurl"
)

func openRedisConn(url string) (redis.Conn, error) {
	if len(url) == 0 {
		return redisurl.Connect()
	}
	return redisurl.ConnectToURL(url)
}

func newRedisStore(url string, dropDatabase bool) (Store, error) {
	c, err := openRedisConn(url)
	if err != nil {
		return nil, debug.Err("open redis connection", err)
	}
	var store = &redisStore{c}
	if dropDatabase {
		store.Do("FLUSHDB")
	}
	return store, nil
}

type redisStore struct {
	conn redis.Conn
}

func (s *redisStore) Begin() (Tx, error) {
	return s, nil
}

func (s *redisStore) Close() error {
	return s.conn.Close()
}

// Tx impl

func (s *redisStore) Commit() error {
	return nil
}

func (s *redisStore) Rollback() error {
	return nil
}

func (s *redisStore) Do(cmd string, args ...interface{}) (interface{}, error) {
	if debug.Enabled() {
		log.Debug("%s %v", cmd, replaceBytes(args))
	}
	v, err := s.conn.Do(cmd, args...)
	if err != nil {
		return nil, debug.Err(cmd, err)
	}
	if debug.Enabled() {
		log.Debug("redis> %v", v)
	}
	return v, nil
}

func replaceBytes(args []interface{}) []interface{} {
	var result []interface{}
	for _, v := range args {
		b, ok := v.([]byte)
		if ok {
			result = append(result, string(b))
		} else {
			result = append(result, v)
		}
	}
	return result
}

func (s *redisStore) Get(k []byte) ([]byte, error) {
	v, err := s.Do("GET", k)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return redis.Bytes(v, nil)
}

func (s *redisStore) Set(k []byte, v []byte) error {
	_, err := s.Do("SET", k, v)
	return err
}

func (s *redisStore) Delete(k []byte) error {
	_, err := s.Do("DEL", k)
	return err
}

func (s *redisStore) Exists(k []byte) (int64, error) {
	return redis.Int64(s.Do("EXISTS", k))
}

func (s *redisStore) GetInt64(k []byte) (int64, error) {
	return redis.Int64(s.Get(k))
}

func (s *redisStore) Incr(k []byte) (int64, error) {
	return redis.Int64(s.Do("INCR", k))
}

func (s *redisStore) Scan(prefix string, cursor int, count int, last []byte) (int, [][]byte, error) {
	v, err := redis.Values(s.Do("SCAN", cursor, "MATCH", prefix+"*", "COUNT", count))
	if err != nil {
		return 0, nil, err
	}

	next, err := redis.Int(v[0], nil)
	if err != nil {
		return 0, nil, debug.Err("redis.Int", err)
	}

	list, err := redis.Strings(v[1], nil)
	if err != nil {
		return 0, nil, debug.Err("redis.Strings", err)
	}

	var keys [][]byte
	for _, s := range list {
		keys = append(keys, []byte(s))
	}
	return next, keys, nil
}
