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
import org.vertx.java.core.json.JsonObject;

import java.net.HttpURLConnection;
import java.util.Map;

/**
 * Registers event bus handlers for CouchDb API methods.
 */
public class CouchDbVerticle extends BusModBase {

    public static final String ADDRESS_ALL_DBS = "/_all_dbs";
    public static final String ADDRESS_ALL_DOCS = "/%1$s/_all_docs";

    private String couchdbHost;
    private int couchdbPort;
    private long timeout;

    /**
     * Registers handlers for all databases and views in the given couchdb instance.
     *
     * @param startedResult
     */
    @Override
    public void start(final Future<Void> startedResult) {
        super.start();

        // configure members
        couchdbHost = getOptionalStringConfig("couchdbHost", "localhost");
        couchdbPort = getOptionalIntConfig("couchdbPort", 5984);
        timeout = getOptionalLongConfig("timeout", 5000);

        // TODO register couchdb server API

        // _all_dbs
        if (logger.isDebugEnabled())
            logger.debug(String.format("registering handler %1$s", ADDRESS_ALL_DBS));
        eb.registerHandler(ADDRESS_ALL_DBS, new AllDbsMessageHandler());
        eb.sendWithTimeout(ADDRESS_ALL_DBS, (Void) null, timeout, new AsyncResultHandler<Message<JsonArray>>() {
            @Override
            public void handle(AsyncResult<Message<JsonArray>> allDbsReply) {
                if (allDbsReply.succeeded()) {
                    for (final Object db : allDbsReply.result().body()) {
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("found db: %1$s", db.toString()));
                        final String allDocsAddress = String.format(ADDRESS_ALL_DOCS, db.toString());

                        // TODO register db handlers

                        // TODO register view handlers
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("registering handler %1$s", allDocsAddress));
                        eb.registerHandler(allDocsAddress, new AllDocsHandler(allDocsAddress));
                    }
                    startedResult.setResult(null);
                } else {
                    startedResult.setFailure(allDbsReply.cause());
                }
            }
        });

    }

    private class AllDocsHandler implements Handler<Message<JsonArray>> {
        private String allDocsAddress;

        private AllDocsHandler(String allDocsAddress) {
            this.allDocsAddress = allDocsAddress;
        }

        @Override
        public void handle(final Message<JsonArray> allDocsMsg) {
            final JsonArray params = allDocsMsg.body();
            final StringBuilder allDocsUri = new StringBuilder(allDocsAddress).append("?");
            for (Object param : params) {
                final JsonObject jsonParam = (JsonObject) param;
                final Map<String, Object> paramMap = jsonParam.toMap();
                for (final String key : paramMap.keySet()) {
                    allDocsUri.append("&").append(key).append("=").append(paramMap.get(key));
                }
            }

            vertx.createHttpClient().setHost(couchdbHost).setPort(couchdbPort).getNow(allDocsUri.toString(),
                    new Handler<HttpClientResponse>() {
                        @Override
                        public void handle(final HttpClientResponse response) {
                            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                                response.bodyHandler(new Handler<Buffer>() {
                                    @Override
                                    public void handle(final Buffer body) {
                                        allDocsMsg.reply(new JsonObject(body.toString("UTF-8")));
                                    }
                                });
                            } else {
                                allDocsMsg.fail(response.statusCode(), response.statusMessage());
                            }
                        }
                    });
        }
    }

    private class AllDbsMessageHandler implements Handler<Message<Void>> {
        @Override
        public void handle(final Message<Void> allDbsMsg) {
            vertx.createHttpClient().setHost(couchdbHost).setPort(couchdbPort).getNow(ADDRESS_ALL_DBS,
                    new Handler<HttpClientResponse>() {
                        @Override
                        public void handle(final HttpClientResponse response) {
                            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                                response.bodyHandler(new Handler<Buffer>() {
                                    @Override
                                    public void handle(Buffer body) {
                                        allDbsMsg.reply(new JsonArray(body.toString("UTF-8")));
                                    }
                                });
                            } else {
                                allDbsMsg.fail(response.statusCode(), response.statusMessage());
                            }
                        }
                    });
        }
    }
}
