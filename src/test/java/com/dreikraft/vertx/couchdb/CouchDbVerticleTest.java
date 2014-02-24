package com.dreikraft.vertx.couchdb;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

/**
 * Created by jan_solo on 24.02.14.
 */
public class CouchDbVerticleTest extends TestVerticle {

    @Override
    public void start() {
        super.initialize();

        container.logger().info(String.format("starting %1$s tests ...", CouchDbVerticle.class.getName()));
        container.deployModule(System.getProperty("vertx.modulename"), new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                container.logger().info(String.format("successfully started %1$s tests",
                        CouchDbVerticle.class.getName()));
                // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
                if (asyncResult.failed()) {
                    container.logger().error(String.format("failed to start %1$s tests",
                            CouchDbVerticle.class.getName()), asyncResult.cause());
                }
                VertxAssert.assertTrue(asyncResult.succeeded());
                VertxAssert.assertNotNull("deploymentID should not be null", asyncResult.result());
                // If deployed correctly then start the tests!
                startTests();
            }
        });
    }

    @Test
    public void testAllDbs() {
        vertx.eventBus().sendWithTimeout(CouchDbVerticle.ADDRESS_ALL_DBS, (Void) null, 5000,
                new AsyncResultHandler<Message<JsonArray>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonArray>> reply) {
                        if (reply.succeeded()) {
                            final JsonArray dbs = reply.result().body();
                            container.logger().info(String.format("/_all_dbs: %1$s", dbs.encode()));
                            VertxAssert.assertTrue("no dbs found", dbs.size() > 0);
                        } else {
                            container.logger().error("failed to perform /_all_dbs", reply.cause());
                            VertxAssert.fail(reply.cause().getMessage());
                        }
                        VertxAssert.testComplete();
                    }
                });
    }

    @Test
    public void testAllDocs() {
        final JsonArray params = new JsonArray("[{\"include_docs\":true}]");
        final String address = String.format(CouchDbVerticle.ADDRESS_ALL_DOCS, "votee");
        vertx.eventBus().sendWithTimeout(address, params, 5000, new AsyncResultHandler<Message<JsonObject>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                        if (reply.succeeded()) {
                            final JsonObject docs = reply.result().body();
                            container.logger().info(String.format("%1$s: %2$s", address, docs.encode()));
                            VertxAssert.assertTrue("no docs found", docs.size() > 0);
                        } else {
                            container.logger().error(String.format("failed to perform %1$s", address),
                                    reply.cause());
                            VertxAssert.fail(reply.cause().getMessage());
                        }
                        VertxAssert.testComplete();
                    }
                });
    }
}
