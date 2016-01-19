package mongo

import (
	"gopkg.in/mgo.v2"
)

type cursor struct {
	iter *mgo.Iter
}

func (c *cursor) Close() error {
	return c.iter.Close()
}

func (c *cursor) Next(result interface{}) (bool, error) {
	var ok = c.iter.Next(result)
	return ok, c.iter.Err()
}
