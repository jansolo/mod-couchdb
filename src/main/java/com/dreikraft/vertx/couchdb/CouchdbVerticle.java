package com.dreikraft.vertx.couchdb;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.net.HttpURLConnection;
import java.util.Map;

/**
 * Registers event bus handlers for CouchDb API methods.
 */
public class CouchdbVerticle extends BusModBase {

    public static final String ADDRESS_DB = "/%1$s";
    public static final String ADDRESS_ALL_DBS = "/_all_dbs";
    public static final String ADDRESS_ALL_DOCS = "/%1$s/_all_docs";
    public static final String ADDRESS_VIEW = "/%1$s/_design/%2$s/_view/%3$s";

    private String couchdbHost;
    private int couchdbPort;
    private long timeout;

    /**
     * Registers handlers for all databases and views in the given couchdb instance.
     *
     * @param startedResult the startup result
     */
    @Override
    public void start(final Future<Void> startedResult) {
        super.start();

        // configure members
        couchdbHost = getOptionalStringConfig("couchdbHost", "localhost");
        couchdbPort = getOptionalIntConfig("couchdbPort", 5984);
        timeout = getOptionalLongConfig("timeout", 10000);

        // TODO register couchdb server API handlers

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
        public void handle(final Message<JsonObject> queryMsg) {
            final JsonObject json = queryMsg.body();
            final StringBuilder queryUri = new StringBuilder(address);
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
            if (logger.isDebugEnabled())
                logger.debug(String.format("executing query: %1$s", queryUri.toString()));

            vertx.createHttpClient().setHost(couchdbHost).setPort(couchdbPort).getNow(queryUri.toString(),
                    new Handler<HttpClientResponse>() {
                        @Override
                        public void handle(final HttpClientResponse response) {
                            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                                response.bodyHandler(new Handler<Buffer>() {
                                    @Override
                                    public void handle(final Buffer body) {
                                        if (body.toString("UTF-8").startsWith("[")) {
                                            queryMsg.reply(new JsonArray(body.toString("UTF-8")));
                                        } else if (body.toString("UTF-8").startsWith("{")) {
                                            queryMsg.reply(new JsonObject(body.toString("UTF-8")));
                                        } else {
                                            queryMsg.reply(body.toString("UTF-8"));
                                        }
                                    }
                                });
                            } else {
                                queryMsg.fail(response.statusCode(), response.statusMessage());
                            }
                        }
                    }).exceptionHandler(new Handler<Throwable>() {
                @Override
                public void handle(final Throwable t) {
                    final String errMsg = String.format("failed to query %1$s: %2$s",
                            queryUri.toString(), t.getMessage());
                    logger.error(errMsg, t);
                    queryMsg.fail(500, errMsg);
                }
            });
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
