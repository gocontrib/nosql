package data

import (
	"strings"

	"github.com/gocontrib/log"
)

// Driver defines interface for data store backends.
type Driver interface {
	Open(url, dbname string) (Store, error)
}

var drivers = make(map[string]Driver)

// RegisterDriver regisers new driver with given names.
func RegisterDriver(driver Driver, knownNames ...string) {
	for _, k := range knownNames {
		k = strings.TrimSpace(k)
		if len(k) == 0 {
			continue
		}
		drivers[strings.ToLower(k)] = driver
	}
	log.Info("registered data store driver: %v", knownNames)
}
