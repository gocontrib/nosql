package tests

import (
	"flag"
	"testing"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/ledis"
)

func TestLedisStore_Basic(t *testing.T) {
	var store = makeLedisStore()
	defer store.Close()
	testBasic(t, store)
}

func TestLedisStore_Cursor(t *testing.T) {
	var store = makeLedisStore()
	defer store.Close()
	testCursor(t, store)
}

func TestLedisStore_Clear(t *testing.T) {
	var store = makeLedisStore()
	defer store.Close()
	testClear(t, store)
}

func TestLedisStore_Filters(t *testing.T) {
	var store = makeLedisStore()
	defer store.Close()
	testFilters(t, store)
}

func BenchmarkLedisStore_Insert(b *testing.B) {
	var store = makeLedisStore()
	defer store.Close()
	benchmarkStoreInsert(b, store, 0)
}

func BenchmarkLedisStore_Read(b *testing.B) {
	var store = makeLedisStore()
	defer store.Close()
	benchmarkStoreRead(b, store)
}

func makeLedisStore() data.Store {
	flag.Parse()

	const (
		path = "data"
		db   = 0
	)

	var store, err = ledis.Open(path, db, true)
	if err != nil {
		log.Fatal("unable to connect to database with %s: %+v", path, err)
	}

	return store
}
