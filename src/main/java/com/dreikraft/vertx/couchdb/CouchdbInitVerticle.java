package com.dreikraft.vertx.couchdb;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * Initializes the couchdb module. Starts the CouchdbVerticle and registers handlers for couchdb db API.
 *
 * @author jansolo
 */
public class CouchdbInitVerticle extends BusModBase {

    /**
     * Starts x instances of the CouchdbVerticle. Optionally creates handlers for all databases found in the configured
     * couchdb server on startup.
     *
     * Supports following config parameters:
     * <ul>
     * <li>instances:<code>int</code> ... Number of CouchdbVerticle instances to start;
     * defaults to number of cpu cores</li>
     * <li>registerDbHandlers:<code>boolean</code> ... register API handlers for the various databases found in the
     * connected couchdb instance on startup; default <code>true</code></li>
     * </ul>
     *
     * @param startedResult the startup result
     */
    @Override
    public void start(final Future<Void> startedResult) {
        super.start();

        container.logger().info(String.format("starting %1$s ...",
                CouchdbInitVerticle.class.getSimpleName()));
        final int instances = getOptionalIntConfig("instances", Runtime.getRuntime().availableProcessors());
        container.logger().info(String.format("starting %1$d %2$s instances ...", instances,
                CouchdbVerticle.class.getName()));
        final long timeout = getOptionalLongConfig("timeout", 10000);

        container.deployVerticle("com.dreikraft.vertx.couchdb.CouchdbVerticle", config, instances,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> deployResult) {
                        if (deployResult.succeeded()) {
                            logger.info(String.format("successfully started %1$d %2$s instances", instances,
                                    CouchdbVerticle.class.getName()));

                            if (getOptionalBooleanConfig("registerDbHandlers", true)) {
                                // register all db handlers
                                eb.send(CouchdbVerticle.ADDRESS_REFLECT, new JsonObject(),
                                        new Handler<Message<JsonObject>>() {
                                            @Override
                                            public void handle(final Message<JsonObject> reflectServerMessage) {
                                                try {
                                                    if (!"error".equals(reflectServerMessage.body().getString
                                                            ("status"))) {
                                                        startedResult.setResult(null);
                                                    } else {
                                                        final Exception ex = new Exception(
                                                                String.format("failed to start CouchdbVerticle: " +
                                                                        "%1$s", reflectServerMessage.body().getString
                                                                        ("message"))
                                                        );
                                                        logger.error(ex.getMessage(), ex);
                                                        startedResult.setFailure(ex);
                                                    }
                                                } catch (RuntimeException ex) {
                                                    logger.error(String.format("failed to start CouchdbVerticle: " +
                                                            "%1$s",ex));
                                                    startedResult.setFailure(ex);
                                                }
                                            }
                                        }
                                );
                            } else {
                                startedResult.setResult(null);
                            }
                        } else {
                            logger.info(String.format("failed to start %1$d %2$s instances", instances,
                                    CouchdbVerticle.class.getSimpleName()));
                            startedResult.setFailure(deployResult.cause());
                        }
                    }
                });
    }
}
