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
 * Tests the CouchdbVerticle.
 *
 * @author jansolo
 */
public class CouchdbVerticleTest extends TestVerticle {

    public static final long TIMEOUT = 300000;
    private static final String DB_NAME = "dummy";

    @Override
    public void start() {
        super.initialize();

        final JsonObject config = new JsonObject().putString("user", "admin").putString("passwd",
                "admin").putNumber("instances", 1).putBoolean("registerDbHandlers", false);
        container.logger().info(String.format("starting %1$s tests ...", CouchdbVerticleTest.class.getName()));
        container.deployModule(System.getProperty("vertx.modulename"), config, new CouchdbTestInitHandler());
    }

    /**
     * Delete the test database and complete the test.
     */
    private void shutdown() {
        final JsonObject deleteDbMsg = new JsonObject().putString("method", "DELETE").putString("db", DB_NAME);
        container.logger().info(String.format("sending message to address %1$s: %2$s",
                CouchdbVerticle.ADDRESS_SERVER, deleteDbMsg));
        vertx.eventBus().sendWithTimeout(CouchdbVerticle.ADDRESS_SERVER, deleteDbMsg, TIMEOUT,
                new AsyncResultHandler<Message<JsonObject>>() {
                    @Override
                    public void handle(final AsyncResult<Message<JsonObject>> reply) {
                        if (reply.succeeded()) {
                            container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                    CouchdbVerticle.ADDRESS_SERVER, reply.result().body())));
                        } else {
                            container.logger().error(String.format("failed to perform %1$s: %2$s",
                                    CouchdbVerticle.ADDRESS_SERVER, reply.cause().getMessage()), reply.cause());
                        }
                        VertxAssert.testComplete();
                    }
                });
    }

    @Test
    public void testAllDbs() {
        vertx.eventBus().sendWithTimeout(CouchdbVerticle.ADDRESS_ALL_DBS, new JsonObject(), TIMEOUT,
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
                        shutdown();
                    }
                });
    }

    @Test
    public void testAllDbDocs() {
        final JsonObject message = new JsonObject().putArray("params", new JsonArray("[{\"include_docs\":true}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_ALL_DOCS, "dummy");
        vertx.eventBus().sendWithTimeout(address, message, TIMEOUT, new AsyncResultHandler<Message<JsonObject>>() {
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
                shutdown();
            }
        });
    }

    @Test
    public void testQueryView() {
        final JsonObject message = new JsonObject().putArray("params",
                new JsonArray("[{\"include_docs\":true}, {\"reduce\":false}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_VIEW, "dummy", "_design/dummy", "all");
        vertx.eventBus().sendWithTimeout(address, message, TIMEOUT, new AsyncResultHandler<Message<JsonObject>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (reply.succeeded()) {
                    final JsonObject docs = reply.result().body();
                    container.logger().info(String.format("%1$s: %2$s", address, docs.encode()));
                    VertxAssert.assertTrue("no docs found", docs.size() > 0);
                } else {
                    container.logger().error(String.format("failed to perform %1$s: %2$s", address,
                            reply.cause().getMessage()), reply.cause());
                    VertxAssert.fail(reply.cause().getMessage());
                }
                shutdown();
            }
        });
    }

    @Test
    public void testGetDoc() {
        final JsonObject message = new JsonObject().putString("id", "dummy1");
        final String address = "/dummy";
        vertx.eventBus().sendWithTimeout(address, message, TIMEOUT, new AsyncResultHandler<Message<JsonObject>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (reply.succeeded()) {
                    final JsonObject docs = reply.result().body();
                    container.logger().info(String.format("%1$s: %2$s", "", docs.encode()));
                    VertxAssert.assertTrue("no docs found", docs.size() > 0);
                } else {
                    container.logger().error(String.format("failed to perform %1$s: %2$s", address,
                            reply.cause().getMessage()), reply.cause());
                    VertxAssert.fail(reply.cause().getMessage());
                }
                shutdown();
            }
        });
    }

    @Test
    public void testCreateDoc() {

        final String serverAddress = CouchdbVerticle.ADDRESS_SERVER;
        final JsonObject createDocMsg = new JsonObject().putString("method", "POST").putString("db",
                DB_NAME).putObject("body", new JsonObject().putString("dummy", "dummy"));
        container.logger().info(String.format("sending message to address %1$s: %2$s",
                CouchdbVerticle.ADDRESS_SERVER, createDocMsg));
        vertx.eventBus().sendWithTimeout(serverAddress, createDocMsg, TIMEOUT,
                new AsyncResultHandler<Message<JsonObject>>() {
                    @Override
                    public void handle(final AsyncResult<Message<JsonObject>> reply) {
                        if (reply.succeeded()) {
                            container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                    serverAddress, reply.result().body())));
                            VertxAssert.assertTrue("not ok", reply.result().body().getBoolean("ok"));
                        } else {
                            container.logger().error(String.format("failed to perform %1$s: %2$s",
                                    serverAddress, reply.cause().getMessage()), reply.cause());
                        }
                        shutdown();
                    }
                });
    }

    private class CouchdbTestInitHandler implements AsyncResultHandler<String> {

        @Override
        public void handle(AsyncResult<String> asyncResult) {
            container.logger().info(String.format("successfully started %1$s tests",
                    CouchdbVerticle.class.getName()));
            // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
            if (asyncResult.failed()) {
                container.logger().error(String.format("failed to start %1$s tests",
                        CouchdbVerticle.class.getName()), asyncResult.cause());
            }
            VertxAssert.assertTrue(asyncResult.succeeded());
            VertxAssert.assertNotNull("deploymentID should not be null", asyncResult.result());

            // create a test database
            final JsonObject createDbMsg = new JsonObject().putString("method", "PUT").putString("db", DB_NAME);
            container.logger().info(String.format("sending message to address %1$s: %2$s",
                    CouchdbVerticle.ADDRESS_SERVER, createDbMsg));
            vertx.eventBus().sendWithTimeout(CouchdbVerticle.ADDRESS_SERVER, createDbMsg, TIMEOUT,
                    new AsyncResultHandler<Message<JsonObject>>() {
                        @Override
                        public void handle(final AsyncResult<Message<JsonObject>> reply) {
                            if (reply.succeeded()) {
                                container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                        CouchdbVerticle.ADDRESS_SERVER, reply.result().body())));

                                // register db handlers for dummy db
                                final String registerDbHandlersAddress = CouchdbVerticle
                                        .ADDRESS_REFLECT;
                                final JsonObject registerDummyHandlersMessage = new
                                        JsonObject().putString("db", DB_NAME);
                                vertx.eventBus().sendWithTimeout(registerDbHandlersAddress,
                                        registerDummyHandlersMessage, TIMEOUT,
                                        new AsyncResultHandler<Message<JsonObject>>() {
                                            @Override
                                            public void handle(final AsyncResult<Message<JsonObject>>
                                                                       registerDbHandlersReply) {
                                                if (registerDbHandlersReply.succeeded()) {
                                                    container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                                            registerDbHandlersAddress, registerDbHandlersReply.result().body())));

                                                    // load test documents and views
                                                    final JsonObject dummySchema = new JsonObject(vertx
                                                            .fileSystem().readFileSync
                                                                    ("dummyDB.json").toString("UTF-8"));
                                                    final JsonObject dummyDbMsg = new JsonObject().putString("method",
                                                            "POST").putObject("body", dummySchema);
                                                    final String bulkLoadAddress = String.format(CouchdbVerticle.ADDRESS_BULK_DOCS,
                                                            DB_NAME);
                                                    container.logger().info(String.format("sending message to address %1$s: %2$s",
                                                            bulkLoadAddress, dummyDbMsg.toString()));
                                                    vertx.eventBus().sendWithTimeout(bulkLoadAddress, dummyDbMsg, TIMEOUT,
                                                            new AsyncResultHandler<Message<JsonArray>>() {
                                                                @Override
                                                                public void handle
                                                                        (final AsyncResult<Message<JsonArray>>
                                                                                 bulkLoadReply) {
                                                                    if (bulkLoadReply.succeeded()) {
                                                                        container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                                                                bulkLoadAddress, bulkLoadReply.result().body())));

                                                                        // reregister view handlers after schema import
                                                                        container.logger().info(String.format("sending message to address %1$s: %2$s",
                                                                                registerDbHandlersAddress, registerDummyHandlersMessage.toString()));
                                                                        vertx.eventBus().sendWithTimeout(registerDbHandlersAddress,
                                                                                registerDummyHandlersMessage, TIMEOUT,
                                                                                new AsyncResultHandler<Message<JsonObject>>() {

                                                                                    @Override
                                                                                    public void handle(final
                                                                                                      AsyncResult<Message<JsonObject>> reregisterReply) {
                                                                                        if (reregisterReply.succeeded
                                                                                                ()) {
                                                                                            container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                                                                                    registerDbHandlersAddress, reregisterReply.result().body())));
                                                                                            // start the tests
                                                                                            startTests();
                                                                                        } else {
                                                                                            container.logger().error(String.format("failed to perform %1$s: %2$s",
                                                                                                    registerDbHandlersAddress,
                                                                                                    registerDbHandlersReply.cause().getMessage()),
                                                                                                    registerDbHandlersReply.cause());
                                                                                        }
                                                                                    }
                                                                                });
                                                                    } else {
                                                                        container.logger().error(String.format("failed to perform %1$s: %2$s",
                                                                                bulkLoadAddress,
                                                                                bulkLoadReply.cause().getMessage()),
                                                                                bulkLoadReply.cause());
                                                                    }
                                                                }
                                                            });
                                                } else {
                                                    container.logger().error(String.format("failed to perform %1$s: %2$s",
                                                            registerDbHandlersAddress,
                                                            registerDbHandlersReply.cause().getMessage()),
                                                            registerDbHandlersReply.cause());
                                                }
                                            }
                                        });

                            } else {
                                container.logger().error(String.format("failed to perform %1$s: %2$s",
                                        CouchdbVerticle.ADDRESS_SERVER, reply.cause().getMessage()), reply.cause());
                            }
                        }
                    });
        }
    }
}
