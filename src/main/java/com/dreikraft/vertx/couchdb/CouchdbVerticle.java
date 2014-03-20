package com.dreikraft.vertx.couchdb;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Wraps the the couchdb API into vert.x event bus calls. Registers event bus handlers for CouchDb API methods.
 *
 * Supported configuration parameters:
 * <ul>
 * <li><code>host: String</code> ... The hostname of the couchdb server; defaults to <code>localhost</code></li>
 * <li><code>port: int</code> ... The port of the couchdb server; defaults to <code>5984</code></li>
 * <li><code>timeout: long</code> ... The request timeout until a call is considered failed; defaults to
 * <code>10000</code> msec</li>
 * <li><code>user: String</code> ... A couchdb username; optional; defaults to <code>null</code></li>
 * <li><code>passwd: String</code> ... A couchdb password; optional; defaults to <code>null</code></li>
 * </ul>
 *
 * Call parameters, http method, database name, request headers and document ids can be passed wrapped into a
 * JsonObject.

 * Get all databases in a couchdb server instance:
 * <ul>
 * <li>address: <code>couchdb:/_all_dbs</code></li>
 * <li>message: <code>{}</code></li>
 * <li>reply: <code>{"body": ["_replicator","_users","dummy","test_suite_db","test_suite_db2"], "status": "ok"}
 * </code></li>
 * </ul>
 *
 * Create a db:
 * <ul>
 * <li>address: <code>couchdb:/</code></li>
 * <li>message: <code>{"method":"PUT","db":"dummy"}</code></li>
 * <li>reply: <code>{"body": {"ok":true}, "status": "ok"}</code></li>
 * </ul>
 *
 * Delete a db:
 * <ul>
 * <li>address: <code>couchdb:/</code></li>
 * <li>message: <code>{"method":"DELETE","db":"dummy"}</code></li>
 * <li>reply: <code>{"body": {"ok":true}, "status": "ok"}</code></li>
 * </ul>
 *
 * Bulk load documents into a database:
 * <ul>
 * <li>address: <code>couchdb:/dummy/_bulk_docs</code></li>
 * <li>message: <code>{"method":"POST","body":{"docs":[{"_id":"dummy1","name":"dummy1"},{"_id":"dummy2","name":"dummy2"}]}}</code></li>
 * <li>reply: <code>{"body": [{"ok":true,"id":"dummy1","rev":"1-8cf73467930ed4ce09baf4067f866696"},{"ok":true,"id":"dummy2","rev":"1-63d558a16704329a6fc5a1f62bef77a3"}], "status": "ok"}</code></li>
 * </ul>
 *
 * Create a document:
 * <ul>
 * <li>address: <code>couchdb:/</code></li>
 * <li>message: <code>{"method":"POST","db":"dummy","body":{"dummy":"dummy"}}</code></li>
 * <li>reply: <code>{"body": {"ok":true,"id":"982ad9b754f4cbce7537729f2800316e","rev":"1-d464c04beb102488a01910290d137c46"}, "status": "ok"}</code></li>
 * </ul>
 *
 * Get a document:
 * <ul>
 * <li>address: <code>couchdb:/dummy</code></li>
 * <li>message: <code>{"id":"dummy1"}</code></li>
 * <li>reply: <code>{"body": {"_id":"dummy1","_rev":"1-8cf73467930ed4ce09baf4067f866696","name":"dummy1"}, "status": "ok"}</code></li>
 * </ul>
 *
 * Query all docs for a view:
 * <ul>
 * <li>address: <code>couchdb:/dummy/_all_docs</code></li>
 * <li>message: <code>{"params":[{"include_docs":true}]}</code></li>
 * <li>reply: <code>{"body": {"total_rows":1,"offset":0,"rows":[{"id":"dummy3","key":"dummy3","value":{"rev":"1-d7e7ace0fb165dcde4d0e9b3de99fbe1"},"doc":{"_id":"dummy3","_rev":"1-d7e7ace0fb165dcde4d0e9b3de99fbe1","name":"dummy3"}}]}, "status": "ok"}</code></li>
 * </ul>
 *
 * Query a view:
 * <ul>
 * <li>address: <code>couchdb:/dummy/_design/dummy/_view/all</code></li>
 * <li>message: <code>{"params":[{"include_docs":true},{"reduce":false}]}</code></li>
 * <li>reply: <code>{"body": {"total_rows":1,"offset":0,"rows":[{"id":"dummy1","key":"dummy1","value":1,"doc":{"_id":"dummy1","_rev":"1-8cf73467930ed4ce09baf4067f866696","name":"dummy1"}}]}, "status": "ok"}</code></li>
 * </ul>
 *
 * Register view handlers for a database:
 * <ul>
 * <li>address: <code>couchdb:/_reflect</code></li>
 * <li>message: <code>{"db":"dummy"}</code></li>
 * <li>reply: <code>{"body": {"ok":true,"count":1}, "status": "ok"}</code></li>
 * </ul>
 *
 * The handler will generate a reply the contains the JSON object/array returned from couchdb. In case of an error
 * the handlers will send a ReplyException with the wrapped couchdb error.
 *
 * @author jansolo
 */
public class CouchdbVerticle extends BusModBase {

    /**
     * Prefix for event bus addresses {@value}.
     */
    public static final String ADDRESS_PREFIX = "couchdb:";
    /**
     * The event bus server address <code>{@value}</code> maps to couchdb API url <code>/</code>.
     */
    public static final String ADDRESS_SERVER = ADDRESS_PREFIX + "/";
    /**
     * The event bus database address <code>{@value}</code> maps to couchdb API url <code>/dbname</code>.
     */
    public static final String ADDRESS_DB = ADDRESS_PREFIX + "/%1$s";
    /**
     * The all databases view address <code>{@value}</code> maps to couchdb API url <code>/_all_dbs</code>.
     */
    public static final String ADDRESS_ALL_DBS = ADDRESS_PREFIX + "/_all_dbs";
    /**
     * The all database documents address <code>{@value}</code> maps to couchdb API url
     * <code>/dbname/_all_docs</code>.
     */
    public static final String ADDRESS_ALL_DOCS = ADDRESS_PREFIX + "/%1$s/_all_docs";
    /**
     * The bulk docs address <code>{@value}</code> maps to couchdb API url <code>/dbname/_bulk_docs</code>.
     */
    public static final String ADDRESS_BULK_DOCS = ADDRESS_PREFIX + "/%1$s/_bulk_docs";
    /**
     * The view addresses <code>{@value}</code> map to couchdb API url
     * <code>/dbname/_design/designDocId/_view/viewName</code>.
     */
    public static final String ADDRESS_VIEW = ADDRESS_PREFIX + "/%1$s/%2$s/_view/%3$s";
    /**
     * The reflect address <code>{@value}</code> registers handlers for a given database or if omitted for all
     * databases in the server.
     */
    public static final String ADDRESS_REFLECT = ADDRESS_PREFIX + "/_reflect";

    private String host;
    private int port;
    private long timeout;
    private String user;
    private String passwd;

    /**
     * Registers handlers for databases and views in a connected couchdb instance.
     *
     * @param startedResult the startup result
     */
    @Override
    public void start(final Future<Void> startedResult) {
        super.start();

        // configure members
        host = getOptionalStringConfig("host", "localhost");
        port = getOptionalIntConfig("port", 5984);
        timeout = getOptionalLongConfig("timeout", 10000);
        user = getOptionalStringConfig("user", null);
        passwd = getOptionalStringConfig("passwd", null);

        // TODO register couchdb server API handlers
        // couchdb server handler
        if (logger.isDebugEnabled())
            logger.debug(String.format("registering handler %1$s", ADDRESS_SERVER));
        eb.registerHandler(ADDRESS_SERVER, new CouchdbRequestHandler(ADDRESS_SERVER));

        // /_all_dbs handler
        if (logger.isDebugEnabled())
            logger.debug(String.format("registering handler %1$s", ADDRESS_ALL_DBS));
        eb.registerHandler(ADDRESS_ALL_DBS, new CouchdbRequestHandler(ADDRESS_ALL_DBS));

        // /_reflect handler
        if (logger.isDebugEnabled())
            logger.debug(String.format("registering handler %1$s", ADDRESS_REFLECT));
        eb.registerHandler(ADDRESS_REFLECT, new ReflectHandler());

        startedResult.setResult(null);
    }

    /**
     * Handles couchdb requests received on the event bus and forwards the calls to couchdb. It registers a response
     * handler that returns the results from couchdb.
     */
    private final class CouchdbRequestHandler implements Handler<Message<JsonObject>> {

        private String address;

        private CouchdbRequestHandler(final String address) {
            this.address = address;
        }

        /**
         * Handles couchdb requests on the event bus.
         *
         * @param requestMsg the request message. E.g.:
         *                   <code>
         *                   {
         *                   "body": {
         *                   "dummy": "dummy"
         *                   },
         *                   "db": "dummy",
         *                   "method": "POST"
         *                   }
         *                   </code>
         */
        @Override
        public void handle(final Message<JsonObject> requestMsg) {
            final JsonObject json = requestMsg.body();
            final StringBuilder couchdbUri = new StringBuilder(address.substring(ADDRESS_PREFIX.length()));
            final String db = json.getString("db");
            if (db != null) {
                couchdbUri.append(address.equals(ADDRESS_SERVER) ? "" : "/").append(db);
            }
            final String id = json.getString("id");
            if (id != null) {
                couchdbUri.append("/").append(id);
            }
            final JsonArray params = json.getArray("params");
            if (params != null) {
                couchdbUri.append("?");
                for (Object param : params) {
                    final JsonObject jsonParam = (JsonObject) param;
                    for (final String key : jsonParam.getFieldNames()) {
                        final Object value = jsonParam.getValue(key);
                        couchdbUri.append("&").append(key).append("=").append(String.class.isAssignableFrom(value
                                .getClass()) ? String.format("\"%1$s\"", value) : value);
                    }
                }
            }
            final JsonArray headers = json.getArray("headers");
            final String method = json.getString("method", "GET");
            final JsonObject body = json.getObject("body");
            final String requestUser = json.getString("user", user);
            final String requestPasswd = json.getString("passwd", passwd);

            if (logger.isDebugEnabled())
                logger.debug(String.format("executing request: %1$s %2$s %3$s", method, couchdbUri.toString(),
                        body != null ? body : ""));

            final HttpClient httpClient = vertx.createHttpClient().setHost(host).setPort(port).exceptionHandler(
                    new RequestExceptionHandler(couchdbUri.toString(), requestMsg));

            final HttpClientRequest request = httpClient.request(method, couchdbUri.toString(),
                    new ResponseHandler(requestMsg));
            putBaseAuth(putBody(putHeaders(request, headers), body), requestUser, requestPasswd).end();
        }

        private HttpClientRequest putBody(final HttpClientRequest request, final JsonObject body) {
            if (body != null) {
                final String bodyText = body.encode();
                request.putHeader("Content-Length", String.valueOf(bodyText.length()))
                        .putHeader("Content-Type", "application/json").write(bodyText, "UTF-8");
            }
            return request;
        }

        private HttpClientRequest putBaseAuth(final HttpClientRequest request, final String user,
                                              final String passwd) {
            if (user != null && passwd != null) {
                request.putHeader("Authorization", new StringBuilder("Basic ").append(
                        new JsonObject().putBinary("baseAuth", String.format("%1$s:%2$s", user, passwd).getBytes())
                                .getString("baseAuth")).toString());
            }
            return request;
        }

        private HttpClientRequest putHeaders(final HttpClientRequest request, final JsonArray headers) {
            if (headers != null) {
                for (final Object header : headers) {
                    final JsonObject headerJson = (JsonObject) header;
                    for (final String headerName : headerJson.getFieldNames()) {
                        request.putHeader(headerName, (String) headerJson.getField(headerName));
                    }
                }
            }
            return request;
        }

        /**
         * Handles responses from couchdb and passes the result into a message reply.
         */
        private final class ResponseHandler implements Handler<HttpClientResponse> {

            private Message<JsonObject> requestMsg;

            private ResponseHandler(Message<JsonObject> requestMsg) {
                this.requestMsg = requestMsg;
            }

            /**
             * Handles couchdb responses.
             *
             * @param response a response from couchdb (a JSON object/array)
             */
            @Override
            public void handle(final HttpClientResponse response) {
                if (response.statusCode() < HttpURLConnection.HTTP_INTERNAL_ERROR) {
                    response.bodyHandler(new Handler<Buffer>() {

                        /**
                         * The result body handler. Returns a JSON object, a JSON array, a String or a ReplyException.
                         * @param body the response body
                         */
                        @Override
                        public void handle(final Buffer body) {
                            if (response.statusCode() >= HttpURLConnection.HTTP_OK
                                    && response.statusCode() < HttpURLConnection.HTTP_MULT_CHOICE) {
                                if (body.toString("UTF-8").startsWith("[")) {
                                    requestMsg.reply(new JsonObject().putArray("body",
                                            new JsonArray(body.toString("UTF-8"))).putString("status", "ok"));
                                } else if (body.toString("UTF-8").startsWith("{")) {
                                    requestMsg.reply(new JsonObject().putObject("body",
                                            new JsonObject(body.toString("UTF-8"))).putString("status", "ok"));
                                } else {
                                    requestMsg.reply(new JsonObject().putString("body",
                                            body.toString("UTF-8")).putString("status", "ok"));
                                }
                            } else {
                                final JsonObject status = new JsonObject(body.toString("UTF-8"));
                                final String statusMsg = String.format("%1$s: %2$s", response.statusMessage(),
                                        status.getString("reason"));
                                sendError(requestMsg, statusMsg);
                            }
                        }
                    });
                } else {
                    sendError(requestMsg, String.format("error: %1$s: %1$s", response.statusCode(), response.statusMessage()));
                }
            }
        }

        /**
         * Handles exceptions while performing the couchdb http request.
         */
        private final class RequestExceptionHandler implements Handler<Throwable> {
            private final String queryUri;
            private final Message<JsonObject> requestMsg;

            /**
             * Creates the handler.
             *
             * @param queryUri   request uri
             * @param requestMsg the request message
             */
            public RequestExceptionHandler(final String queryUri, final Message<JsonObject> requestMsg) {
                this.queryUri = queryUri;
                this.requestMsg = requestMsg;
            }

            /**
             * Logs the exception and returns a ReplyException on the request message
             *
             * @param t a causing exception
             */
            @Override
            public void handle(final Throwable t) {
                final String errMsg = String.format("failed to query %1$s: %2$s", queryUri, t.getMessage());
                sendError(requestMsg, errMsg, (Exception) t);
            }
        }
    }

    /**
     * Performs a reflection a database or all databases in a couchdb server. Finds all db/view urls and registers
     * handlers for the urls.
     */
    private final class ReflectHandler implements Handler<Message<JsonObject>> {

        private int dbsProcessed;
        private int dbsCount;
        private Message<JsonObject> reflectServerMsg;
        private Map<String, Set<HandlerEntry>> dbsHandlerEntries;

        private ReflectHandler() {
            dbsHandlerEntries = new HashMap<>();
        }

        /**
         * Handles the <code>/_reflect</code> event.
         *
         * @param reflectServerMsg
         */
        @Override
        public void handle(final Message<JsonObject> reflectServerMsg) {
            this.reflectServerMsg = reflectServerMsg;
            final String db = reflectServerMsg.body().getString("db");
            dbsProcessed = 0;
            if (db != null) {
                dbsCount = 1;
                registerDbHandlers(db);
            } else {
                eb.sendWithTimeout(ADDRESS_ALL_DBS, new JsonObject(), timeout,
                        new AsyncResultHandler<Message<JsonObject>>() {
                            @Override
                            public void handle(final AsyncResult<Message<JsonObject>> allDbsReply) {
                                if (allDbsReply.succeeded()) {
                                    final JsonObject json = allDbsReply.result().body();
                                    if (logger.isDebugEnabled())
                                        logger.debug(String.format("found dbs: %1$s ", json.encode()));
                                    if (!"error".equals(json.getString("status"))) {
                                        final JsonArray dbs = json.getArray("body");
                                        dbsCount = dbs.size();
                                        for (final Object dbObj : dbs) {
                                            registerDbHandlers(dbObj.toString());
                                        }
                                        replyOnComplete();
                                    } else {
                                        final String errMsg = String.format("failed to reflect couchdb server: %1$s",
                                                json.getString("message"));
                                        sendError(reflectServerMsg, errMsg);
                                    }
                                } else {
                                    final String errMsg = String.format("failed to reflect couchdb server: %1$s",
                                            allDbsReply.cause().getMessage());
                                    sendError(reflectServerMsg, errMsg);
                                }
                            }
                        });
            }
        }

        private void replyOnComplete() {
            if (dbsProcessed == dbsCount) {
                reflectServerMsg.reply(
                        new JsonObject().putObject("body", new JsonObject().putNumber("count",
                                dbsCount)).putString("status", "ok"));
            }
        }

        private void registerDbHandlers(final String db) {

            if (dbsHandlerEntries.containsKey(db)) {
                for (final HandlerEntry dbHandlerEntry : dbsHandlerEntries.get(db)) {
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("unregistering handler %1$s", dbHandlerEntry.getAddress()));
                    eb.unregisterHandler(dbHandlerEntry.getAddress(), dbHandlerEntry.getHandler());
                }
            }
            final Set<HandlerEntry> dbHandlerEntries = new HashSet<>();
            dbsHandlerEntries.put(db, dbHandlerEntries);

            // /db/doc handler
            final String dbAddress = String.format(ADDRESS_DB, db);
            dbHandlerEntries.add(new HandlerEntry(dbAddress, new CouchdbRequestHandler(dbAddress)));

            // /db/_all_docs handler
            final String allDocsAddress = String.format(ADDRESS_ALL_DOCS, db);
            dbHandlerEntries.add(new HandlerEntry(allDocsAddress, new CouchdbRequestHandler(allDocsAddress)));

            // /db/_bulk_docs handler
            final String bulkDocsAddress = String.format(ADDRESS_BULK_DOCS, db);
            dbHandlerEntries.add(new HandlerEntry(bulkDocsAddress, new CouchdbRequestHandler(bulkDocsAddress)));

            for (final HandlerEntry dbHandlerEntry : dbsHandlerEntries.get(db)) {
                if (logger.isDebugEnabled())
                    logger.debug(String.format("registering handler %1$s", dbHandlerEntry.getAddress()));
                eb.registerHandler(dbHandlerEntry.getAddress(), dbHandlerEntry.getHandler());
            }

            // TODO register missing db/doc API handlers

            // /db/_design/docid/_view/viewname handlers
            registerViewHandlers(db);
        }

        private void registerViewHandlers(final String db) {
            // get design docs and views
            final String allDocsAddress = String.format(ADDRESS_ALL_DOCS, db);
            final JsonObject designDocsMessage = new JsonObject().putArray("params",
                    new JsonArray().add(new JsonObject().putString("startkey", "_design")).add(new JsonObject()
                            .putString("endkey", "_e")).add(new JsonObject().putBoolean("include_docs", true)));
            eb.sendWithTimeout(allDocsAddress, designDocsMessage, timeout,
                    new QueryDesignDocsHandler(db));
        }

        /**
         * A handler to register handler addresses on the event bus for all design documents views found in a database.
         */
        private final class QueryDesignDocsHandler implements AsyncResultHandler<Message<JsonObject>> {

            private final String db;

            public QueryDesignDocsHandler(final String db) {
                this.db = db;
            }

            /**
             * Handles found design docs.
             *
             * @param designDocsResult design docs for a database returned from couchdb.
             */
            @Override
            public void handle(final AsyncResult<Message<JsonObject>> designDocsResult) {
                if (designDocsResult.succeeded()) {
                    final JsonObject json = designDocsResult.result().body();
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("design doc: %1$s", json.encodePrettily()));
                    if (!"error".equals(json.getString("status"))) {
                        final JsonArray rows = json.getObject("body").getArray("rows");
                        for (final Object row : rows) {
                            final JsonObject designDoc = ((JsonObject) row).getObject("doc");
                            final JsonObject views = designDoc.getObject("views");
                            if (views != null && views.getFieldNames() != null) {
                                for (final String viewName : views.getFieldNames()) {
                                    if (logger.isDebugEnabled())
                                        logger.debug(String.format("view name: %1$s", viewName));
                                    final String viewURI = String.format(ADDRESS_VIEW, db,
                                            ((JsonObject) row).getString("id"), viewName);

                                    // /db/_design/docid/_view/viewname handler
                                    if (logger.isDebugEnabled())
                                        logger.debug(String.format("registering handler %1$s", viewURI));
                                    final Handler<Message<JsonObject>> viewHandler = new CouchdbRequestHandler(viewURI);
                                    dbsHandlerEntries.get(db).add(new HandlerEntry(viewURI, viewHandler));
                                    eb.registerHandler(viewURI, viewHandler);
                                }
                            }
                        }
                    } else {
                        logger.error(String.format("failed to query design docs for db %1$s: %2$s",
                                db, json.getString("message")));
                    }
                } else {
                    logger.error(String.format("failed to query design docs for db %1$s",
                            db), designDocsResult.cause());
                }
                dbsProcessed++;
                replyOnComplete();
            }
        }

        /**
         * Stores address/handler mappings required for deregistering handlers.
         */
        private final class HandlerEntry {
            private final String address;
            private final Handler<Message<JsonObject>> handler;

            private HandlerEntry(final String address, final Handler<Message<JsonObject>> handler) {
                this.address = address;
                this.handler = handler;
            }

            /**
             * Returns the address of the handler.
             *
             * @return a address string
             */
            public String getAddress() {
                return address;
            }

            /**
             * Returns the vert.x event handler.
             *
             * @return a handler
             */
            public Handler<Message<JsonObject>> getHandler() {
                return handler;
            }
        }
    }
}
