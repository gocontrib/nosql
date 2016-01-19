package postgresql

import (
	"database/sql"
	"encoding/json"
	"strconv"

	"github.com/gocontrib/nosql/reflection"
)

type cursor struct {
	store *store
	rows  *sql.Rows
}

func (c *cursor) Close() error {
	return c.rows.Close()
}

func (c *cursor) Next(result interface{}) (bool, error) {
	if !c.rows.Next() {
		return false, c.rows.Err()
	}
	var id int64
	var data []byte
	var err = c.rows.Scan(&id, &data)
	if err != nil {
		return false, err
	}
	err = json.Unmarshal(data, result)
	if err != nil {
		return false, err
	}
	var meta = reflection.GetMeta(result)
	var sid = strconv.FormatInt(id, 10)
	meta.SetID(result, sid)
	return true, nil
}
