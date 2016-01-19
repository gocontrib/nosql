package reflection

import (
	"reflect"
	"strings"
	"sync"
)

// Meta getters/setters used in data store implementation.
type Meta struct {
	GetID        Getter
	SetID        Setter
	SetCreatedAt Setter
	SetUpdatedAt Setter
}

// MakeMeta gets meta for given type.
func MakeMeta(t reflect.Type) Meta {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	var m = Meta{
		SetID:        NoopSetter,
		SetCreatedAt: NoopSetter,
		SetUpdatedAt: NoopSetter,
	}

	for i := 0; i < t.NumField(); i++ {
		var f = t.Field(i)
		if strings.ToLower(f.Name) == "id" {
			m.GetID = MakeGetter(f)
			m.SetID = MakeSetter(f)
			continue
		}
		if f.Name == "CreatedAt" {
			m.SetCreatedAt = MakeSetter(f)
			continue
		}
		if f.Name == "UpdatedAt" {
			m.SetUpdatedAt = MakeSetter(f)
			continue
		}
	}

	return m
}

// cache of Meta objects.
type metaCacheImpl struct {
	sync.Mutex
	meta map[reflect.Type]Meta
}

// GetMeta for given object or type.
func (c metaCacheImpl) GetMeta(target interface{}) *Meta {
	c.Lock()
	defer c.Unlock()
	t, ok := target.(reflect.Type)
	if !ok {
		t = reflect.TypeOf(target)
	}
	if c.meta == nil {
		c.meta = make(map[reflect.Type]Meta)
	}
	m, ok := c.meta[t]
	if !ok {
		m = MakeMeta(t)
		c.meta[t] = m
	}
	return &m
}

var meteCache = &metaCacheImpl{
	meta: make(map[reflect.Type]Meta),
}

// GetMeta for given object or type.
func GetMeta(target interface{}) *Meta {
	return meteCache.GetMeta(target)
}
