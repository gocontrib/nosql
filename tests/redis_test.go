package tests

import (
	"flag"
	"testing"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/redis"
)

func TestRedisStore_Basic(t *testing.T) {
	var store = makeRedisStore()
	defer store.Close()
	testBasic(t, store)
}

func TestRedisStore_Cursor(t *testing.T) {
	var store = makeRedisStore()
	defer store.Close()
	testCursor(t, store)
}

func TestRedisStore_Clear(t *testing.T) {
	var store = makeRedisStore()
	defer store.Close()
	testClear(t, store)
}

func TestRedisStore_Filters(t *testing.T) {
	var store = makeRedisStore()
	defer store.Close()
	testFilters(t, store)
}

func BenchmarkRedisStore_Insert(b *testing.B) {
	var store = makeRedisStore()
	defer store.Close()
	benchmarkStoreInsert(b, store, 0)
}

func BenchmarkRedisStore_Read(b *testing.B) {
	var store = makeRedisStore()
	defer store.Close()
	benchmarkStoreRead(b, store)
}

func makeRedisStore() data.Store {
	flag.Parse()

	const (
		url = "tcp://127.0.0.1:6379/15"
	)

	var store, err = redis.Open(url, true)
	if err != nil {
		log.Fatal("unable to connect to redis with %s: %+v", url, err)
	}

	return store
}
