package tests

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/postgresql"
)

func TestPostgreStore_Basic(t *testing.T) {
	var store = makePgStore()
	defer store.Close()
	testBasic(t, store)
}

func TestPostgreStore_Cursor(t *testing.T) {
	var store = makePgStore()
	defer store.Close()
	testCursor(t, store)
}

func TestPostgreStore_Clear(t *testing.T) {
	var store = makePgStore()
	defer store.Close()
	testClear(t, store)
}

func TestPostgreStore_Filters(t *testing.T) {
	var store = makePgStore()
	defer store.Close()
	testFilters(t, store)
}

func BenchmarkPostgreStore_Insert(b *testing.B) {
	var store = makePgStore()
	defer store.Close()
	benchmarkStoreInsert(b, store, 0)
}

func BenchmarkPostgreStore_Read(b *testing.B) {
	var store = makePgStore()
	defer store.Close()
	benchmarkStoreRead(b, store)
}

func makePgStore() data.Store {
	flag.Parse()

	var pwd = os.Getenv("PGPWD")
	if len(pwd) == 0 {
		log.Fatal("PGPWD env is not set")
	}

	var (
		dburl  = fmt.Sprintf("user=postgres password=%s sslmode=disable", pwd)
		dbname = "test"
	)

	var store, err = postgresql.Open(dburl, dbname, true)
	if err != nil {
		log.Fatal("unable to connect to database with %s: %+v", dburl, err)
	}

	return store
}
