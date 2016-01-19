# nosql
Unified interface to NoSQL data stores.

## Supported data store backend

* [boltdb](https://github.com/boltdb/bolt)
* [ledisdb](https://github.com/siddontang/ledisdb) which suports a lot of backends
* mongodb using gopkg.in/mgo.v2
* postgresql using github.com/lib/pq based on JSONB data type
* redis like data stores (TODO list them codis, etc)

## TODO
* stabilization (need contribution)
* need Tx interface for multiple changes in one transaction
* discuss and improve API
* unit tests
* [ ] optimizations
	* [ ] postgresql
		* [ ] query cache
		* [ ] sql builder
		* [ ] maybe reduce use of fmt
