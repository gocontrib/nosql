package data

// Store of document collections.
type Store interface {
	// Collection returns collection by name.
	Collection(name string) Collection
	// Close performs cleanups.
	Close() error
}

// Collection of documents.
type Collection interface {
	// Name of collection.
	Name() string
	// Count returns number of documents in the collection.
	Count() (int64, error)
	// Insert given documents to the collection.
	Insert(docs ...interface{}) error
	// Gets one result by id.
	Get(id string, result interface{}) error
	// Gets all results.
	GetAll(result interface{}) error
	// Find opens new query session.
	Find(filter ...interface{}) Result
	// Update given document.
	Update(selector interface{}, doc interface{}) error
	// Delete documents that match given filter.
	Delete(selector interface{}) error
}

// Result set.
type Result interface {
	// Count returns the number of items that match the set conditions.
	Count() (int64, error)
	// One fetches the first result within the result set.
	One(interface{}) error
	// All fetches all results within the result set.
	All(interface{}) error
	// Limit defines the maximum number of results in this set.
	Limit(int64) Result
	// Skip ignores first *n* results.
	Skip(int64) Result
	// Sort results by given fields.
	Sort(...string) Result
	// Cursor executes query and returns cursor capable of going over all the results.
	Cursor() (Cursor, error)
}

// Cursor API
type Cursor interface {
	// Close closes the cursor, preventing further enumeration.
	Close() error
	// Next reads the next result.
	Next(result interface{}) (bool, error)
}
