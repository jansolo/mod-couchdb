# mod-couchdb

Wraps the couchdb API onto vert.x event bus messages. The module registers handlers for querying and updating
couchdb databases on the event bus. The module can be used to query and update couchdb directly from the browser via
the event bus bridge (be careful which calls you expose over the bridge, because the module itself does not enforce
and check any security), but typically will be wrapped behind some "business" verticles.

See JavaDoc on the CouchdbVerticle for more information on the registered handlers and usage of the event bus API.

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

# Configuration

The module supports following configuration parameters:

- `host: String` ... The hostname of the couchdb server; defaults to `localhost`
- `port: int` ... The port of the couchdb server; defaults to `5984`
- `user: String` ... A couchdb username; optional; defaults to `null`
- `passwd: String` ... A couchdb password; optional; defaults to `null`
- `registerDbHandlers: boolean`... Whether to register handlers for all databases found in the configured couchdb
server at startup of the module; defaults to `true
- `instances: boolean`... The number of verticle instances, that should be started; defaults to the number of processor
cores on the system

# Usage

First handlers for the CouchDB API need to be registered by the module. This will be done automatically at startup for
 all databases found in the configured server. If you don't like to register handlers for all databases, you can set the
`registerDbHandlers` to `false` in the module config. Then you need to register the handlers manually by sending a
message on the event with the name of the database you want to register the handlers for:

- address: `couchdb:/_reflect`
- message: `{"db":"dummy"}`
- reply: `{"body": {"ok":true,"count":1}, "status": "ok"}`

All handlers support a set of parameters, that will be mapped to the corresponding couchdb API calls - not all
parameters make sense on all API calls. The parameters need to be send to the matching event bus address wrapped into
 a JSON Object. Following parameters are supported:

 ```
 {
    "db": "the name of the database",
    "method": "the http method passed to couchdb GET/PUT/DELETE/HEAD",
    "headers": [ an array of http headers passed to couchdb  ]
    "params": [ an array of url query parameters passed to couchdb ],
    "id": "a document id",
    "body": { a json object passed in the request body to couchdb },
    "user": " a couchdb basic auth user name",
    "passwd": " a couchdb basic auth user password"
 }
 ```

The response from couchdb gets wrapped into a JSON object and will by replied to the caller:

```
{
    "status": "ok/error",
    "message": " an error message from couchdb in case of an failed request",
    "body": { a json object/array containing the result from couchdb}
}
```

The `status` field can be used to check if the call was successful ("ok"), otherwise the reply will contain a `message`
field with the corresponding error message from couchdb.

## Sample calls

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


