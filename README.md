# nosql
Unified interface to NoSQL data stores.

## Supported data store backends

* [boltdb](https://github.com/boltdb/bolt)
* [ledisdb](https://github.com/siddontang/ledisdb) which suports a lot of backends
* mongodb using gopkg.in/mgo.v2
* postgresql using github.com/lib/pq based on JSONB data type
* redis like data stores (TODO list them codis, etc)

## API

```go
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
```

## Query example

Query model looks like gopkg.in/mgo.v2 MongoDB driver.

```go
import "fmt"
import "github.com/gocontrib/nosql"
import "github.com/gocontrib/nosql/q"

type Message struct {
	ID 			string `json:"id"`
	UserID 	string `json:"user_id"`
	Body 		string `json:"body"`
}

func main() {
	// configure and get store
	store := nosql.GetStore()

	// get document collection
	messages := store.Collection("messages")

	// query all messages from given user
	var messages []Message
	err := messages.Find(q.M{"user_id": "1"}).All(&messages)
	if err != nil {
		panic(err)
	}
	for _, m := range messages {
		fmt.Println(m.Body)
	}
}

```

## TODO
* configuration and better api to create Store instance
* stabilization (need contribution)
* need Tx interface for multiple changes in one transaction
* discuss and improve API
* unit tests
* [ ] optimizations
	* [ ] postgresql
		* [ ] query cache
		* [ ] sql builder
		* [ ] maybe reduce use of fmt
