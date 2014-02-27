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

/**
 * Registers event bus handlers for CouchDb API methods.
 */
public class CouchdbVerticle extends BusModBase {

    public static final String ADDRESS_SERVER = "/";
    public static final String ADDRESS_DB = "/%1$s";
    public static final String ADDRESS_ALL_DBS = "/_all_dbs";
    public static final String ADDRESS_ALL_DOCS = "/%1$s/_all_docs";
    public static final String ADDRESS_VIEW = "/%1$s/_design/%2$s/_view/%3$s";

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
        eb.sendWithTimeout(ADDRESS_ALL_DBS, new JsonObject(), timeout, new DbReflectionHandler(startedResult));
    }

    private class CouchdbRequestHandler implements Handler<Message<JsonObject>> {

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
            final String method = json.getString("method", "GET");
            final JsonObject body = json.getObject("body");

            if (logger.isDebugEnabled())
                logger.debug(String.format("executing request: %1$s %2$s %3$s", method, couchdbUri.toString(),
                        body != null ? body : ""));

            final HttpClient httpClient = vertx.createHttpClient().setHost(host).setPort(port);
            final Handler<HttpClientResponse> responseHandler = new ResponseHandler(requestMsg);
            final Handler<Throwable> requestExcHandler = new RequestExceptionHandler(couchdbUri, requestMsg);
            if (method.equals("GET")) {
                final HttpClientRequest request = httpClient.get(couchdbUri.toString(), responseHandler)
                        .exceptionHandler(requestExcHandler);
                putBaseAuth(request).end();
            } else if (method.equals("HEAD")) {
                final HttpClientRequest request = httpClient.head(couchdbUri.toString(), responseHandler)
                        .exceptionHandler(requestExcHandler);
                putBaseAuth(request).end();
            } else if (method.equals("DELETE")) {
                final HttpClientRequest request = httpClient.delete(couchdbUri.toString(), responseHandler)
                        .exceptionHandler(requestExcHandler);
                putBaseAuth(request).end();
            } else if (method.equals("PUT")) {
                final HttpClientRequest request = httpClient.put(couchdbUri.toString(), responseHandler)
                        .exceptionHandler(requestExcHandler);
                putBaseAuth(putBody(body, request)).end();
            } else if (method.equals("POST")) {
                final HttpClientRequest request = httpClient.post(couchdbUri.toString(), responseHandler)
                        .exceptionHandler(requestExcHandler);
                putBaseAuth(putBody(body, request)).end();
            }
        }

        private HttpClientRequest putBody(final JsonObject body, final HttpClientRequest request) {
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

        private class ResponseHandler implements Handler<HttpClientResponse> {

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

        private class RequestExceptionHandler implements Handler<Throwable> {
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

    private class DbReflectionHandler implements AsyncResultHandler<Message<JsonArray>> {

        private final Future<Void> startedResult;
        private boolean completed = false;
        private int dbCount = 0;

        public DbReflectionHandler(final Future<Void> startedResult) {
            this.startedResult = startedResult;
        }

        @Override
        public void handle(final AsyncResult<Message<JsonArray>> allDbsReply) {
            if (allDbsReply.succeeded()) {
                final boolean excludeSysDbs = getOptionalBooleanConfig("excludeSysDbs", true);
                for (final Object db : allDbsReply.result().body()) {
                    if (!(excludeSysDbs && db.toString().startsWith("_"))) {

                        // /db/doc handler
                        final String dbName = db.toString();
                        final String dbAddress = String.format(ADDRESS_DB, dbName);
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("registering handler %1$s", dbAddress));
                        eb.registerHandler(dbAddress, new CouchdbRequestHandler(dbAddress));

                        // /db/_all_docs handler
                        final String allDocsAddress = String.format(ADDRESS_ALL_DOCS, dbName);
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("registering handler %1$s", allDocsAddress));
                        eb.registerHandler(allDocsAddress, new CouchdbRequestHandler(allDocsAddress));

                        // TODO register missing db/doc API handlers

                        // /db/_design/docid/_view/viewname handlers
                        registerViewHandlers(allDocsAddress, dbName, startedResult);
                    }
                }
                completed = true;
                checkCompleted();
            } else {
                logger.error(String.format("failed reflect couchdb: %1$s", allDbsReply.cause().getMessage()),
                        allDbsReply.cause());
                startedResult.setFailure(allDbsReply.cause());
            }
        }

        private void registerViewHandlers(final String allDocsAddress, final String db,
                                          final Future<Void> startedResult) {
            if (logger.isDebugEnabled())
                logger.debug(String.format("registering handler %1$s", allDocsAddress));

            // get design docs and views
            final JsonObject designDocsMessage = new JsonObject().putArray("params",
                    new JsonArray().add(new JsonObject().putString("startkey", "_design")).add(new JsonObject()
                            .putString("endkey", "_e")).add(new JsonObject().putBoolean("include_docs", true)));
            dbCount++;
            eb.sendWithTimeout(allDocsAddress, designDocsMessage, timeout,
                    new QueryDesignDocsHandler(db));
        }

        private void checkCompleted() {
            if (completed && dbCount == 0) {
                startedResult.setResult(null);
            }
        }

        private class QueryDesignDocsHandler implements AsyncResultHandler<Message<JsonObject>> {

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
                        for (final String viewName : views.getFieldNames()) {
                            if (logger.isDebugEnabled())
                                logger.debug(String.format("view name: %1$s", viewName));
                            final String viewURI = String.format("/%1$s/%2$s/_view/%3$s", db,
                                    ((JsonObject) row).getString("id"), viewName);

                            // /db/_design/docid/_view/viewname handler
                            if (logger.isDebugEnabled())
                                logger.debug(String.format("registering handler %1$s", viewURI));
                            eb.registerHandler(viewURI, new CouchdbRequestHandler(viewURI));
                        }
                    }
                    dbCount--;
                    checkCompleted();
                } else {
                    logger.error(String.format("failed to query design docs for db %1$s",
                            db), designDocsResult.cause());
                }
            }
        }
    }
}
