package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gocontrib/nosql"
	"github.com/gocontrib/nosql/q"

	"github.com/stretchr/testify/assert"
)

var perf = false

type User struct {
	ID        string    `json:"id" bson:"_id"`
	Name      string    `json:"name" bson:"name"`
	Email     string    `json:"email" bson:"email"`
	Age       int64     `json:"age" bson:"age"`
	CreatedAt time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time `json:"updated_at" bson:"updated_at"`
}

func ok(t *testing.T, op string, err error) {
	if err != nil {
		t.Errorf(op+" failed with: %v", err)
		t.FailNow()
	}
}

func testBasic(t *testing.T, store data.Store) {
	assert := assert.New(t)
	var users = store.Collection("users")
	assert.NotNil(users)

	// testing basic CRUD operations

	var bob = User{
		Name:  "bob",
		Email: "bob@mail.net",
		Age:   20,
	}

	var err = users.Insert(&bob)
	ok(t, "insert", err)

	if len(bob.ID) == 0 {
		t.Error("ID is not set")
		t.FailNow()
	}

	count, err := users.Count()
	ok(t, "count", err)

	assert.Equal(int64(1), count)

	var now = time.Now().UTC()
	assert.WithinDuration(now, bob.CreatedAt, 500*time.Millisecond, "created_at is not set")
	assert.WithinDuration(now, bob.UpdatedAt, 500*time.Millisecond, "updated_at is not set")

	var usr User
	err = users.Get(bob.ID, &usr)
	ok(t, "get", err)
	assertUser(t, bob, usr)

	bob.Name = "rob"
	bob.Email = "rob@mail.net"
	err = users.Update(bob.ID, &bob)
	ok(t, "update", err)

	err = users.Get(bob.ID, &usr)
	ok(t, "get", err)
	assertUser(t, bob, usr)

	err = users.Delete(bob.ID)
	ok(t, "delete", err)

	count, err = users.Count()
	ok(t, "count", err)

	assert.Equal(int64(0), count)
}

func testClear(t *testing.T, store data.Store) {
	var _, err = insertTestUsers(store, 5)
	ok(t, "insert", err)
	clear(t, store.Collection("users"))
}

func testFilters(t *testing.T, store data.Store) {
	assert := assert.New(t)
	var users = store.Collection("users")
	assert.NotNil(users)

	// testing cursor, filters

	var bob = User{
		Name:  "bob",
		Email: "bob@mail.net",
		Age:   20,
	}
	var rob = User{
		Name:  "rob",
		Email: "rob@mail.net",
		Age:   25,
	}
	var ben = User{
		Name:  "ben",
		Email: "ben@mail.net",
		Age:   30,
	}

	var err = users.Insert(&bob, &rob, &ben)
	ok(t, "insert", err)

	count, err := users.Count()
	ok(t, "count", err)
	assert.Equal(int64(3), count)

	var all = []User{bob, rob, ben}
	print("users: ", all)

	var found = []User{}
	err = users.GetAll(&found)
	ok(t, "get all", err)
	assertUsers(t, found, all)

	found = []User{}
	err = users.Find().All(&found)
	ok(t, "find all", err)
	assertUsers(t, found, all)

	found = []User{}
	err = users.Find().Limit(2).All(&found)
	ok(t, "find limit 2", err)
	assertUsers(t, found, []User{bob, rob})

	found = []User{}
	err = users.Find().Skip(2).All(&found)
	ok(t, "find skip 2", err)
	assertUsers(t, found, []User{ben})

	found = []User{}
	err = users.Find().Sort("name").All(&found)
	ok(t, "find all sort by name", err)
	assertUsers(t, found, []User{ben, bob, rob})

	found = []User{}
	err = users.Find().Sort("-name").All(&found)
	ok(t, "find all sort by -name", err)
	assertUsers(t, found, []User{rob, bob, ben})

	testFindUser(t, users, "bob", bob)
	testFindUser(t, users, bob.ID, bob)
	testFindUser(t, users, bob.Email, bob)

	testFindUsers(t, users, q.In{"bob", "rob"}, []User{bob, rob})

	testFindAll(t, users, q.And{
		q.M{"age": q.GTE(20)},
		q.M{"age": q.LTE(25)},
	}, []User{bob, rob})
}

func testCursor(t *testing.T, store data.Store) {
	assert := assert.New(t)

	var n = 111
	all, err := insertTestUsers(store, n)
	ok(t, "insert", err)

	var lookup = make(map[string]User)
	for _, u := range all {
		lookup[u.ID] = u
	}

	var users = store.Collection("users")
	cur, err := users.Find().Cursor()
	ok(t, "cursor", err)

	var usr User

	for i := 0; i < n; i++ {
		has, err := cur.Next(&usr)
		ok(t, "cursor next", err)
		assert.True(has)
		expected, exists := lookup[usr.ID]
		assert.True(exists, fmt.Sprintf("user %s does not exist", usr.ID))
		assertUser(t, usr, expected)
	}

	has, err := cur.Next(&usr)
	ok(t, "cursor next", err)
	assert.False(has)
}

func clear(t *testing.T, c data.Collection) {
	assert := assert.New(t)

	err := c.Delete(nil)
	ok(t, "delete", err)

	count, err := c.Count()
	ok(t, "count", err)

	assert.Equal(int64(0), count)
}

func testFindUser(t *testing.T, users data.Collection, login string, expected User) {
	var filter = q.Or{
		q.M{"id": login},
		q.M{"name": login},
		q.M{"email": login},
	}
	testFindOne(t, users, filter, expected)
}

func testFindUsers(t *testing.T, users data.Collection, logins q.In, expected []User) {
	var filter = q.Or{
		q.M{"id": logins},
		q.M{"name": logins},
		q.M{"email": logins},
	}
	testFindAll(t, users, filter, expected)
}

func testFindOne(t *testing.T, users data.Collection, filter interface{}, expected User) {
	print("filter: ", filter)
	var usr User
	var err = users.Find(filter).One(&usr)
	ok(t, "find one", err)
	print("found: ", usr)
	assertUser(t, usr, expected)
}

func testFindAll(t *testing.T, users data.Collection, filter interface{}, expected []User) {
	print("filter:", filter)
	var found []User
	var err = users.Find(filter).All(&found)
	ok(t, "find all", err)
	print("found: ", found)
	assertUsers(t, found, expected)
}

var debugPrint = false

func print(prefix string, v interface{}) {
	if debugPrint {
		var json, _ = json.MarshalIndent(v, "", "  ")
		fmt.Println(prefix + string(json))
	}
}

func assertUsers(t *testing.T, found []User, expected []User) {
	assert := assert.New(t)
	assert.Equal(len(expected), len(found))
	for _, e := range expected {
		var exists = false
		for _, u := range found {
			assert.True(len(u.ID) > 0, "id is not set")
			if u.ID == e.ID {
				exists = true
				assertUser(t, e, u)
				break
			}
		}
		assert.True(exists, fmt.Sprintf("user %s <%s> does not exists", e.Name, e.Email))
	}
}

func assertUser(t *testing.T, actual User, expected User) {
	assert := assert.New(t)
	assert.Equal(expected.ID, actual.ID)
	assert.Equal(expected.Name, actual.Name)
	assert.Equal(expected.Email, actual.Email)
	assert.Equal(expected.Age, actual.Age)
}

func insertTestUsers(store data.Store, n int) ([]User, error) {
	var now = time.Now()
	var users = store.Collection("users")
	var list []User
	for i := 0; i < n; i++ {
		var name = fmt.Sprintf("user%d", i+1)
		var email = fmt.Sprintf("%s@mail.net", name)
		var user = User{
			Name:  name,
			Email: email,
			Age:   int64(20 + i),
		}
		var err = users.Insert(&user)
		if err != nil {
			return nil, err
		}
		list = append(list, user)
	}

	if perf {
		fmt.Printf("inserted %d users in %v\n", n, time.Since(now))
	}

	count, err := users.Count()
	if err != nil {
		return nil, err
	}
	if count != int64(n) {
		return nil, errors.New("invalid user count")
	}
	return list, nil
}

func benchmarkStoreInsert(b *testing.B, store data.Store, n int) {
	if n <= 0 {
		n = b.N
	}
	_, err := insertTestUsers(store, n)
	if err != nil {
		b.Errorf("insert failed with %v", err)
		b.FailNow()
	}
}

func benchmarkStoreRead(b *testing.B, store data.Store) {
	var n = 1000
	benchmarkStoreInsert(b, store, n)
	b.ResetTimer()
	var users = store.Collection("users")
	for k := 0; k < b.N; k++ {
		var i = rand.Intn(n - 1)
		var name = fmt.Sprintf("user%d", i+1)
		var email = fmt.Sprintf("%s@mail.net", name)
		var usr User
		var err = users.Find(q.Or{
			q.M{"id": name},
			q.M{"name": name},
			q.M{"email": name},
		}).One(&usr)
		if err != nil {
			b.Errorf("find one failed with %v", err)
			b.FailNow()
		}
		err = users.Find(q.Or{
			q.M{"id": email},
			q.M{"name": email},
			q.M{"email": email},
		}).One(&usr)
		if err != nil {
			b.Errorf("find one failed with %v", err)
			b.FailNow()
		}
	}
}
