package postgresql

import (
	"database/sql"
	"fmt"

	"github.com/gocontrib/log"
	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/util"

	// load postgresql driver
	_ "github.com/lib/pq"
)

var (
	debug = log.IfDebug("postgresql")
)

type driver struct{}

func (d *driver) Open(url, db string) (data.Store, error) {
	return Open(url, db, false)
}

func init() {
	data.RegisterDriver(&driver{}, "pg", "postgre", "postgresql")
}

// Open postgresql data store.
// dropDatabase only for testing use
func Open(connectionURL string, databaseName string, dropDatabase bool) (data.Store, error) {
	constr := util.ReplaceEnv(connectionURL)
	var db, err = sql.Open("postgres", constr)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if dropDatabase {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", databaseName))
		debug.Error("unable to drop database: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", databaseName))
	if err != nil {
		debug.Error("unable to create database: %v", err)
	}

	db, err = sql.Open("postgres", constr+" dbname="+databaseName)
	if err != nil {
		return nil, err
	}
	var store = &store{
		db:   db,
		name: databaseName,
	}
	return store, nil
}

type store struct {
	db   *sql.DB
	name string
}

// Collection returns collection by name.
func (s *store) Collection(name string) data.Collection {
	return &collection{
		store: s,
		db:    s.db,
		name:  name,
	}
}

// Close performs cleanups.
func (s *store) Close() error {
	return s.db.Close()
}
