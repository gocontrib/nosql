package tests

import (
	"flag"
	"testing"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/mongo"
)

func TestMongoStore_Basic(t *testing.T) {
	var store = makeMongoStore()
	defer store.Close()
	testBasic(t, store)
}

func TestMongoStore_Cursor(t *testing.T) {
	var store = makeMongoStore()
	defer store.Close()
	testCursor(t, store)
}

func TestMongoStore_Clear(t *testing.T) {
	var store = makeMongoStore()
	defer store.Close()
	testClear(t, store)
}

func TestMongoStore_Filters(t *testing.T) {
	var store = makeMongoStore()
	defer store.Close()
	testFilters(t, store)
}

func BenchmarkMongoStore_Insert(b *testing.B) {
	var store = makeMongoStore()
	defer store.Close()
	benchmarkStoreInsert(b, store, 0)
}

func BenchmarkMongoStore_Read(b *testing.B) {
	var store = makeMongoStore()
	defer store.Close()
	benchmarkStoreRead(b, store)
}

func makeMongoStore() data.Store {
	flag.Parse()

	const (
		dburl  = "127.0.0.1"
		dbname = "test"
	)

	var store, err = mongo.Open(dburl, dbname, true)
	if err != nil {
		log.Fatal("unable to connect to database with %s: %+v", dburl, err)
	}

	return store
}
