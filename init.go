package data

import (
	"strings"

	"github.com/gocontrib/log"
)

func noop() {}

var (
	// cleanup function.
	cleanup = noop

	store Store
)

// GetStore instance.
func GetStore() Store {
	return store
}

// setStore initializes global data store
func setStore(s Store) {
	if s == nil {
		log.Fatal("data store is not initialized")
	}
	log.Info("data store initialized")
	store = s
	cleanup = func() {
		store.Close()
		log.Info("data store closed")
	}
}

// Cleanup closes connections, etc.
func Cleanup() {
	cleanup()
	cleanup = noop
}

// Init data store.
func Init(driver, url, dbname string) {
	log.Info("init data store")

	d, ok := drivers[strings.ToLower(driver)]
	if !ok {
		log.Fatal("unknown database driver")
	}

	s, err := d.Open(url, dbname)
	if err != nil {
		log.Fatal("unable to connect to %s database with %s: %+v", driver, url, err)
	}

	setStore(s)
}
