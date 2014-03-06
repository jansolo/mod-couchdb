# mod-couchdb

Wraps the couchdb API onto vert.x event bus messages. See JavaDoc on the CouchdbVerticle for more information on the
registered handlers and usage of the event bus API.

# Dependencies

The module does not have any external dependencies. If you want to run the unit tests a configured and running couchdb
is required.

# Installation

## Module

Deploy the module `jansolo~mod-couchdb~0.1-SNAPSHOT` in the `start()` method of your application verticle.

## Standalone

```
vertx runmod 3kraft~mod-couchdb~0.1-SNAPSHOT
```

## Configuration

- `host: String` ... The hostname of the couchdb server; defaults to `localhost`
- `port: int` ... The port of the couchdb server; defaults to `5984`
- `timeout: long` ... The request timeout until a call is considered failed; defaults to `10000` msec
- `user: String` ... A couchdb username; optional; defaults to `null`
- `passwd: String` ... A couchdb password; optional; defaults to `null`

# Usage

Wraps the the couchdb API into vert.x event bus calls. Registers event bus handlers for CouchDb API methods. Call 
parameters, http method, database name, request headers and document ids can be passed wrapped into a JsonObject.

### Get all databases in a couchdb server instance:

- address: `couchdb:/_all_dbs`
- message: `{}`
- reply: `["_replicator","_users","dummy","test_suite_db","test_suite_db2"]`

### Create a db:

- address: `couchdb:/`
- message: `{"method":"PUT","db":"dummy"}`
- reply: `{"ok":true}`

### Delete a db:

- address: `couchdb:/`
- message: `{"method":"DELETE","db":"dummy"}`
- reply: `{"ok":true}`

### Bulk load documents into a database:

- address: `couchdb:/dummy/_bulk_docs`
- message: `{"method":"POST","body":{"docs":[{"_id":"dummy1","name":"dummy1"},{"_id":"dummy2","name":"dummy2"}]}}`
- reply: `[{"ok":true,"id":"dummy1","rev":"1-8cf73467930ed4ce09baf4067f866696"},{"ok":true,"id":"dummy2","rev":"1-63d558a16704329a6fc5a1f62bef77a3"}]`

### Create a document:

- address: `couchdb:/`
- message: `{"method":"POST","db":"dummy","body":{"dummy":"dummy"}}`
- reply: `{"ok":true,"id":"982ad9b754f4cbce7537729f2800316e","rev":"1-d464c04beb102488a01910290d137c46"}`

### Get a document:

- address: `couchdb:/dummy`
- message: `{"id":"dummy1"}`
- reply: `{"_id":"dummy1","_rev":"1-8cf73467930ed4ce09baf4067f866696","name":"dummy1"}`

### Query all docs for a view:

- address: `couchdb:/dummy/_all_docs`
- message: `{"params":[{"include_docs":true}]}`
- reply: `{"total_rows":1,"offset":0,"rows":[{"id":"dummy3","key":"dummy3","value":{"rev":"1-d7e7ace0fb165dcde4d0e9b3de99fbe1"},"doc":{"_id":"dummy3","_rev":"1-d7e7ace0fb165dcde4d0e9b3de99fbe1","name":"dummy3"}}]}`

### Query a view:

- address: `couchdb:/dummy/_design/dummy/_view/all`
- message: `{"params":[{"include_docs":true},{"reduce":false}]}`
- reply: `{"total_rows":1,"offset":0,"rows":[{"id":"dummy1","key":"dummy1","value":1,"doc":{"_id":"dummy1","_rev":"1-8cf73467930ed4ce09baf4067f866696","name":"dummy1"}}]}`

### Register view handlers for a database:

- address: `couchdb:/_reflect`
- message: `{"db":"dummy"}`
- reply: `{"ok":true,"count":1}`

The handler will generate a reply the contains the JSON object/array returned from couchdb. In case of an error
the handlers will send a ReplyException with the wrapped couchdb error.

