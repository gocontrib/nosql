package redis

import (
	"strconv"

	"github.com/gocontrib/log"
)

const keyRangeLimit = 100

type cursor struct {
	bucket      *bucket
	err         error
	initialized bool
	lastID      int64
	keys        [][]byte
	idx         int
	current     int
	next        int
}

func (c *cursor) init() {
	if c.initialized {
		return
	}
	c.initialized = true

	id, err := c.bucket.LastID()
	if err != nil {
		c.err = err
		return
	}
	c.lastID = id

	c.scan(0, nil)
}

func (c *cursor) scan(cursor int, last []byte) error {
	if c.err != nil {
		return c.err
	}

	next, keys, err := c.bucket.tx.Scan(c.bucket.prefix, cursor, keyRangeLimit, last)

	if err != nil {
		c.err = err
		debug.Err("scan", err)
		return err
	}

	c.keys = keys
	c.idx = 0
	c.current = cursor
	c.next = next

	return nil
}

func logKeys(keys [][]byte) {
	var a []string
	for _, v := range keys {
		a = append(a, string(v))
	}
	log.Debug("ledis: keys %v", a)
}

func (c *cursor) makeKey(id int64) []byte {
	return []byte(c.bucket.prefix + strconv.FormatInt(id, 10))
}

func (c *cursor) First() ([]byte, []byte) {
	c.init()

	if c.current != 0 {
		c.scan(0, nil)
	}

	if c.err != nil {
		return nil, nil
	}

	c.idx = 0

	if c.idx < len(c.keys) {
		return c.seek(c.keys[c.idx])
	}

	return nil, nil
}

func (c *cursor) Next() ([]byte, []byte) {
	var inc = 1
	if !c.initialized {
		inc = 0
	}

	c.init()

	if c.err != nil {
		return nil, nil
	}

	c.idx = c.idx + inc

	if c.idx < len(c.keys) {
		return c.seek(c.keys[c.idx])
	}

	// get next range
	if c.next > 0 && c.lastID > 0 && len(c.keys) > 0 {
		c.scan(c.next, c.keys[c.idx-1])
		if c.err != nil {
			return nil, nil
		}
		if c.idx < len(c.keys) {
			return c.seek(c.keys[c.idx])
		}
	}

	return nil, nil
}

func (c *cursor) Seek(k []byte) ([]byte, []byte) {
	if k == nil || c.err != nil {
		return nil, nil
	}
	// add prefix
	k = []byte(c.bucket.prefix + string(k))
	return c.seek(k)
}

// key must be prefixed in seek function
func (c *cursor) seek(k []byte) ([]byte, []byte) {
	var v, err = c.bucket.tx.Get(k)
	if err != nil {
		c.err = err
		debug.Err("seek", err)
		return nil, nil
	}
	if v == nil {
		return nil, nil
	}
	// remove prefix
	k = k[len(c.bucket.prefix):]
	return k, v
}
