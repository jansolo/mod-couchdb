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
 * Registers event bus handlers for CouchDb API methods.
 */
public class CouchdbVerticle extends BusModBase {

    public static final String ADDRESS_SERVER = "/";
    public static final String ADDRESS_DB = "/%1$s";
    public static final String ADDRESS_ALL_DBS = "/_all_dbs";
    public static final String ADDRESS_ALL_DOCS = "/%1$s/_all_docs";
    public static final String ADDRESS_BULK_DOCS = "/%1$s/_bulk_docs";
    public static final String ADDRESS_VIEW = "/%1$s/%2$s/_view/%3$s";
    public static final String ADDRESS_REFLECT = "/_reflect";


    private String host;
    private int port;
    private long timeout;
    private String baseAuth;

    /**
     * Registers handlers for all databases and views in the given couchdb instance.
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
        final String user = getOptionalStringConfig("user", null);
        final String passwd = getOptionalStringConfig("passwd", null);
        if (user != null && passwd != null) {
            baseAuth = new StringBuilder("Basic ").append(new JsonObject().putBinary("baseAuth",
                    String.format("%1$s:%2$s", user, passwd).getBytes()).getString("baseAuth")).toString();
        }

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

        if (getOptionalBooleanConfig("registerDbHandlers", true)) {
            // register all db handlers
            eb.sendWithTimeout(ADDRESS_REFLECT, new JsonObject(), timeout,
                    new AsyncResultHandler<Message<JsonObject>>() {
                        @Override
                        public void handle(final AsyncResult<Message<JsonObject>> reflectServerMessage) {
                            if (reflectServerMessage.succeeded()) {
                                startedResult.setResult(null);
                            } else {
                                logger.error(String.format("failed to start CouchdbVerticle: %1$s",
                                        reflectServerMessage.cause()));
                                startedResult.setFailure(reflectServerMessage.cause());
                            }
                        }
                    });
        } else {
            startedResult.setResult(null);
        }
    }

    private final class CouchdbRequestHandler implements Handler<Message<JsonObject>> {

        private String address;

        private CouchdbRequestHandler(final String address) {
            this.address = address;
        }

        @Override
        public void handle(final Message<JsonObject> requestMsg) {
            final JsonObject json = requestMsg.body();
            final StringBuilder couchdbUri = new StringBuilder(address);
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

            if (logger.isDebugEnabled())
                logger.debug(String.format("executing request: %1$s %2$s %3$s", method, couchdbUri.toString(),
                        body != null ? body : ""));

            final HttpClient httpClient = vertx.createHttpClient().setHost(host).setPort(port).exceptionHandler(
                    new RequestExceptionHandler(couchdbUri, requestMsg));

            final HttpClientRequest request = httpClient.request(method, couchdbUri.toString(),
                    new ResponseHandler(requestMsg));
            putBaseAuth(putBody(putHeaders(request, headers), body)).end();
        }

        private HttpClientRequest putBody(final HttpClientRequest request, final JsonObject body) {
            if (body != null) {
                final String bodyText = body.encode();
                request.putHeader("Content-Length", String.valueOf(bodyText.length()))
                        .putHeader("Content-Type", "application/json").write(bodyText, "UTF-8");
            }
            return request;
        }

        private HttpClientRequest putBaseAuth(final HttpClientRequest request) {
            if (baseAuth != null) {
                request.putHeader("Authorization", baseAuth);
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

        private final class ResponseHandler implements Handler<HttpClientResponse> {

            private Message<JsonObject> requestMsg;

            private ResponseHandler(Message<JsonObject> requestMsg) {
                this.requestMsg = requestMsg;
            }

            @Override
            public void handle(final HttpClientResponse response) {
                if (response.statusCode() < HttpURLConnection.HTTP_INTERNAL_ERROR) {
                    response.bodyHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(final Buffer body) {
                            if (response.statusCode() >= HttpURLConnection.HTTP_OK
                                    && response.statusCode() < HttpURLConnection.HTTP_MULT_CHOICE) {
                                if (body.toString("UTF-8").startsWith("[")) {
                                    requestMsg.reply(new JsonArray(body.toString("UTF-8")));
                                } else if (body.toString("UTF-8").startsWith("{")) {
                                    requestMsg.reply(new JsonObject(body.toString("UTF-8")));
                                } else {
                                    requestMsg.reply(body.toString("UTF-8"));
                                }
                            } else {
                                final JsonObject status = new JsonObject(body.toString("UTF-8"));
                                final String statusMsg = String.format("%1$s: %2$s", response.statusMessage(),
                                        status.getString("reason"));
                                requestMsg.fail(response.statusCode(), statusMsg);
                            }
                        }
                    });

                } else {
                    requestMsg.fail(response.statusCode(), response.statusMessage());
                }
            }
        }

        private final class RequestExceptionHandler implements Handler<Throwable> {
            private final StringBuilder queryUri;
            private final Message<JsonObject> requestMsg;

            public RequestExceptionHandler(StringBuilder queryUri, Message<JsonObject> requestMsg) {
                this.queryUri = queryUri;
                this.requestMsg = requestMsg;
            }

            @Override
            public void handle(final Throwable t) {
                final String errMsg = String.format("failed to query %1$s: %2$s",
                        queryUri.toString(), t.getMessage());
                logger.error(errMsg, t);
                requestMsg.fail(500, errMsg);
            }
        }
    }

    private final class ReflectHandler implements Handler<Message<JsonObject>> {

        private int dbsProcessed;
        private int dbsCount;
        private Message<JsonObject> reflectServerMsg;
        private Map<String, Set<HandlerEntry>> dbsHandlerEntries;

        private ReflectHandler() {
            dbsHandlerEntries = new HashMap<>();
        }

        @Override
        public void handle(final Message<JsonObject> reflectServerMsg) {
            this.reflectServerMsg = reflectServerMsg;
            final String db = reflectServerMsg.body().getString("db");
            dbsProcessed = 0;
            if (db != null) {
                dbsCount = 1;
                registerDbHandlers(db);
            } else {
                eb.sendWithTimeout(ADDRESS_ALL_DBS, new JsonObject(), timeout, new AsyncResultHandler<Message<JsonArray>>() {
                    @Override
                    public void handle(final AsyncResult<Message<JsonArray>> allDbsReply) {
                        if (allDbsReply.succeeded()) {
                            dbsCount = allDbsReply.result().body().size();
                            for (final Object dbObj : allDbsReply.result().body()) {
                                registerDbHandlers(dbObj.toString());
                            }
                            replyOnComplete();
                        } else {
                            final String errMsg = String.format("failed to reflect couchdb server: %1$s",
                                    allDbsReply.cause().getMessage());
                            logger.error(errMsg, allDbsReply.cause());
                            reflectServerMsg.fail(500, allDbsReply.cause().getMessage());
                        }
                    }
                });
            }
        }

        private void replyOnComplete() {
            if (dbsProcessed == dbsCount) {
                reflectServerMsg.reply(new JsonObject().putBoolean("ok", true).putNumber("count", dbsCount));
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

        private final class QueryDesignDocsHandler implements AsyncResultHandler<Message<JsonObject>> {

            private final String db;

            public QueryDesignDocsHandler(final String db) {
                this.db = db;
            }

            @Override
            public void handle(final AsyncResult<Message<JsonObject>> designDocsResult) {
                if (designDocsResult.succeeded()) {
                    final JsonObject json = designDocsResult.result().body();
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("design doc: %1$s", json.encodePrettily()));
                    final JsonArray rows = json.getArray("rows");
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
                    logger.error(String.format("failed to query design docs for db %1$s",
                            db), designDocsResult.cause());
                }
                dbsProcessed++;
                replyOnComplete();
            }
        }

        private final class HandlerEntry {
            private final String address;
            private final Handler<Message<JsonObject>> handler;

            private HandlerEntry(final String address, final Handler<Message<JsonObject>> handler) {
                this.address = address;
                this.handler = handler;
            }

            public String getAddress() {
                return address;
            }

            public Handler<Message<JsonObject>> getHandler() {
                return handler;
            }
        }

    }
}
