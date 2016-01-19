package mongo

import (
	"github.com/gocontrib/nosql"
	"gopkg.in/mgo.v2"
)

type driver struct{}

func (d *driver) Open(url, dbname string) (data.Store, error) {
	return Open(url, dbname, false)
}

func init() {
	data.RegisterDriver(&driver{}, "mongo", "mongodb")
}

// Open mongo data store.
func Open(url, databaseName string, dropDatabase bool) (data.Store, error) {
	var session, err = mgo.Dial(url)
	if err != nil {
		return nil, err
	}
	var store = &store{
		session: session,
		dbname:  databaseName,
	}
	if dropDatabase {
		err = store.Drop()
		if err != nil {
			return nil, err
		}
	}
	return store, nil
}

type store struct {
	session *mgo.Session
	dbname  string
}

// Collection returns collection by name.
func (s *store) Collection(name string) data.Collection {
	return &collection{s, name}
}

// Close performs cleanups.
func (s *store) Close() error {
	s.session.Close()
	return nil
}

// Drops underlying database. For testing purposes.
func (s *store) Drop() error {
	return s.session.DB(s.dbname).DropDatabase()
}
