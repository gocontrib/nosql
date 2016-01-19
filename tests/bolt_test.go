package tests

import (
	"flag"
	"testing"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/bolt"
)

func TestBoltStore_Basic(t *testing.T) {
	var store = makeBoltStore()
	defer store.Close()
	testBasic(t, store)
}

func TestBoltStore_Cursor(t *testing.T) {
	var store = makeBoltStore()
	defer store.Close()
	testCursor(t, store)
}

func TestBoltStore_Clear(t *testing.T) {
	var store = makeBoltStore()
	defer store.Close()
	testClear(t, store)
}

func TestBoltStore_Filters(t *testing.T) {
	var store = makeBoltStore()
	defer store.Close()
	testFilters(t, store)
}

func BenchmarkBoltStore_Insert(b *testing.B) {
	var store = makeBoltStore()
	defer store.Close()
	benchmarkStoreInsert(b, store, 0)
}

func BenchmarkBoltStore_Read(b *testing.B) {
	var store = makeBoltStore()
	defer store.Close()
	benchmarkStoreRead(b, store)
}

func makeBoltStore() data.Store {
	flag.Parse()

	const (
		path = "test.db"
	)

	var store, err = bolt.Open(path, true)
	if err != nil {
		log.Fatal("unable to connect to database with %s: %+v", path, err)
	}

	return store
}
