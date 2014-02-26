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
            final StringBuilder queryUri = new StringBuilder(address);
            final String db = json.getString("db");
            if (db != null) {
                queryUri.append(address.equals(ADDRESS_SERVER) ? "" : "/").append(db);
            }
            final String id = json.getString("id");
            if (id != null) {
                queryUri.append("/").append(id);
            }
            final JsonArray params = json.getArray("params");
            if (params != null) {
                queryUri.append("?");
                for (Object param : params) {
                    final JsonObject jsonParam = (JsonObject) param;
                    for (final String key : jsonParam.getFieldNames()) {
                        final Object value = jsonParam.getValue(key);
                        queryUri.append("&").append(key).append("=").append(String.class.isAssignableFrom(value
                                .getClass()) ? String.format("\"%1$s\"", value) : value);
                    }
                }
            }
            final String method = json.getString("method", "GET");
            final JsonObject body = json.getObject("body");

            if (logger.isDebugEnabled())
                logger.debug(String.format("executing query: %1$s", queryUri.toString()));

            final HttpClient httpClient = vertx.createHttpClient().setHost(host).setPort(port);
            final Handler<HttpClientResponse> responseHandler = new ResponseHandler(requestMsg);
            final Handler<Throwable> exceptionHandler = new ConnectExceptionHandler(queryUri, requestMsg);
            if (method.equals("GET")) {
                final HttpClientRequest request = httpClient.get(queryUri.toString(), responseHandler)
                        .exceptionHandler(exceptionHandler);
                putBaseAuth(request).end();
            } else if (method.equals("HEAD")) {
                final HttpClientRequest request = httpClient.head(queryUri.toString(), responseHandler)
                        .exceptionHandler(exceptionHandler);
                putBaseAuth(request).end();
            } else if (method.equals("DELETE")) {
                final HttpClientRequest request = httpClient.delete(queryUri.toString(), responseHandler)
                        .exceptionHandler(exceptionHandler);
                putBaseAuth(request).end();
            } else if (method.equals("PUT")) {
                final HttpClientRequest request = httpClient.put(queryUri.toString(), responseHandler)
                        .exceptionHandler(exceptionHandler);
                putBaseAuth(putBody(body, request)).end();
            } else if (method.equals("POST")) {
                final HttpClientRequest request = httpClient.post(queryUri.toString(), responseHandler)
                        .exceptionHandler(exceptionHandler);
                putBaseAuth(putBody(body, request)).end();
            }
        }

        private HttpClientRequest putBody(final JsonObject body, final HttpClientRequest request) {
            if (body != null) {
                final String bodyText = body.encode();
                request.putHeader("Content-Length", String.valueOf(bodyText.length())).write(bodyText,
                        "UTF-8");
            } else {
                request.putHeader("Content-Length", "0");
            }
            return request.putHeader("Content-Type", "application/json");
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
                if (response.statusCode() >= HttpURLConnection.HTTP_OK
                        && response.statusCode() < HttpURLConnection.HTTP_MULT_CHOICE) {
                    response.bodyHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(final Buffer body) {
                            if (body.toString("UTF-8").startsWith("[")) {
                                requestMsg.reply(new JsonArray(body.toString("UTF-8")));
                            } else if (body.toString("UTF-8").startsWith("{")) {
                                requestMsg.reply(new JsonObject(body.toString("UTF-8")));
                            } else {
                                requestMsg.reply(body.toString("UTF-8"));
                            }
                        }
                    });
                } else {
                    requestMsg.fail(response.statusCode(), response.statusMessage());
                }

            }
        }

        private class ConnectExceptionHandler implements Handler<Throwable> {
            private final StringBuilder queryUri;
            private final Message<JsonObject> requestMsg;

            public ConnectExceptionHandler(StringBuilder queryUri, Message<JsonObject> requestMsg) {
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
            } else {
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
            eb.sendWithTimeout(allDocsAddress, designDocsMessage, timeout,
                    new QueryDesignDocsHandler(db, startedResult));
        }

        private class QueryDesignDocsHandler implements AsyncResultHandler<Message<JsonObject>> {

            private final String db;
            private final Future<Void> startedResult;

            public QueryDesignDocsHandler(final String db, final Future<Void> startedResult) {
                this.db = db;
                this.startedResult = startedResult;
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
                            if (logger.isDebugEnabled())
                                logger.debug(String.format("registering handler %1$s", viewURI));

                            // /db/_design/docid/_view/viewname handler
                            if (logger.isDebugEnabled())
                                logger.debug(String.format("registering handler %1$s", viewURI));
                            eb.registerHandler(viewURI, new CouchdbRequestHandler(viewURI));
                        }
                    }
                    startedResult.setResult(null);
                } else {
                    logger.error(String.format("failed to query design docs for db %1$s",
                            db), designDocsResult.cause());
                    startedResult.setFailure(designDocsResult.cause());
                }
            }
        }
    }
}
