package com.dreikraft.vertx.couchdb;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;

/**
 * Created by jan_solo on 24.02.14.
 */
public class CouchdbInitVerticle extends BusModBase {

    @Override
    public void start(final Future<Void> startedResult) {
        super.start();

        container.logger().info(String.format("starting %1$s ...",
                CouchdbInitVerticle.class.getSimpleName()));
        final int instances = getOptionalIntConfig("instances", Runtime.getRuntime().availableProcessors());
        container.logger().info(String.format("starting %1$d %2$s instances ...", instances,
                CouchdbVerticle.class.getName()));

        container.deployVerticle("com.dreikraft.vertx.couchdb.CouchdbVerticle", config, instances,
                new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> deployResult) {
                        if (deployResult.succeeded()) {
                            logger.info(String.format("successfully started %1$d %2$s instances", instances,
                                    CouchdbVerticle.class.getName()));
                            startedResult.setResult(null);
                        } else {
                            logger.info(String.format("failed to start %1$d %2$s instances", instances,
                                    CouchdbVerticle.class.getSimpleName()));
                            startedResult.setFailure(deployResult.cause());
                        }
                    }
                });
    }
}