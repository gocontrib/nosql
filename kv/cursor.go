package kv

type cursor struct {
	view   *view
	tx     Tx
	bkt    Bucket
	iter   Iter
	closed bool
}

func (c *cursor) transaction() Tx { return c.tx }
func (c *cursor) bucket() Bucket  { return c.bkt }
func (c *cursor) key() []byte     { return c.iter.Key() }
func (c *cursor) value() []byte   { return c.iter.Value() }

func (c *cursor) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true

	var err = c.tx.Commit()
	if err != nil {
		return err
	}

	return c.tx.Rollback()
}

func (c *cursor) Next(result interface{}) (bool, error) {
	if !c.next() {
		return false, nil
	}
	var err = c.view.unmarshal(c, result, nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *cursor) next() bool {
	if c.closed {
		return false
	}

	ok, err := c.iter.Next()
	if err != nil {
		c.closed = true
		c.tx.Rollback()
		return false
	}

	if !ok {
		c.Close()
		return false
	}

	return true
}
