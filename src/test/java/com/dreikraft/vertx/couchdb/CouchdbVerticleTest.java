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

    public static final long TIMEOUT = 30000;

    @Override
    public void start() {
        super.initialize();

        final JsonObject config = new JsonObject().putString("user", "admin").putString("passwd",
                "admin").putNumber("instances", 8);
        container.logger().info(String.format("starting %1$s tests ...", CouchdbVerticleTest.class.getName()));
        container.deployModule(System.getProperty("vertx.modulename"), config, new AsyncResultHandler<String>() {
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
                // If deployed correctly then start the tests!
                startTests();
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
                        VertxAssert.testComplete();
                    }
                });
    }

    @Test
    public void testAllDocs() {
        final JsonObject message = new JsonObject().putArray("params", new JsonArray("[{\"include_docs\":true}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_ALL_DOCS, "votee");
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
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void testQueryView() {
        final JsonObject message = new JsonObject().putArray("params",
                new JsonArray("[{\"include_docs\":true}, {\"reduce\":false}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_VIEW, "votee", "votee", "events");
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
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void testGetDoc() {
        final JsonObject message = new JsonObject().putString("id", "te1");
        final String address = "/votee";
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
                VertxAssert.testComplete();
            }
        });
    }

    @Test
    public void testCreateDb() {

        final String serverAddress = CouchdbVerticle.ADDRESS_SERVER;
        final JsonObject createDbMsg = new JsonObject().putString("method", "PUT").putString("db", "dummy");
        final JsonObject createDocMsg = new JsonObject().putString("method", "POST").putString("db",
                "dummy").putObject("body", new JsonObject().putString("dummy", "dummy"));
        final JsonObject deleteDbMsg = new JsonObject().putString("method", "DELETE").putString("db", "dummy");
        container.logger().info(String.format("executing next request %1$s %2$s", serverAddress, createDbMsg));
        vertx.eventBus().sendWithTimeout(serverAddress, createDbMsg, TIMEOUT,
                new DefaultChainableHandler(serverAddress, deleteDbMsg, new DefaultChainableHandler()) {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                        if (reply.succeeded()) {
                            final JsonObject replyBody = reply.result().body();
                            container.logger().info(replyBody);
                            VertxAssert.assertTrue("not ok", replyBody.getBoolean("ok"));
                        } else {
                            container.logger().error(String.format("failed to process: %1$s",
                                    reply.cause().getMessage()),
                                    reply.cause());
                            VertxAssert.fail(reply.cause().getMessage());
                        }
                        sendNext();
                    }
                }
        );
    }

    @Test
    public void testCreateDoc() {

        final String serverAddress = CouchdbVerticle.ADDRESS_SERVER;
        final JsonObject createDbMsg = new JsonObject().putString("method", "PUT").putString("db", "dummy");
        final JsonObject createDocMsg = new JsonObject().putString("method", "POST").putString("db",
                "dummy").putObject("body", new JsonObject().putString("dummy", "dummy"));
        final JsonObject deleteDbMsg = new JsonObject().putString("method", "DELETE").putString("db", "dummy");
        container.logger().info(String.format("executing next request %1$s %2$s", serverAddress, createDbMsg));
        vertx.eventBus().sendWithTimeout(serverAddress, createDbMsg, TIMEOUT,
                new DefaultChainableHandler(serverAddress, createDocMsg,
                        new ChainableHandler(serverAddress, deleteDbMsg, new DefaultChainableHandler()) {
                            @Override
                            public void handle(AsyncResult<Message<JsonObject>> reply) {
                                if (reply.succeeded()) {
                                    final JsonObject replyBody = reply.result().body();
                                    container.logger().info(replyBody);
                                    VertxAssert.assertTrue("not ok", replyBody.getBoolean("ok"));
                                } else {
                                    container.logger().error(String.format("failed to process: %1$s",
                                            reply.cause().getMessage()),
                                            reply.cause());
                                    VertxAssert.fail(reply.cause().getMessage());
                                }
                                sendNext();
                            }
                        }
                ));
    }

    private abstract class ChainableHandler implements AsyncResultHandler<Message<JsonObject>> {

        protected final String nextAddress;
        protected final JsonObject nextMsg;
        protected final AsyncResultHandler<Message<JsonObject>> nextHandler;

        private ChainableHandler(final String nextAddress, final JsonObject nextMsg,
                                 final AsyncResultHandler<Message<JsonObject>> nextHandler) {
            this.nextAddress = nextAddress;
            this.nextMsg = nextMsg;
            this.nextHandler = nextHandler;
        }

        @Override
        public abstract void handle(AsyncResult<Message<JsonObject>> reply);

        protected void sendNext() {
            if (nextHandler != null) {
                container.logger().info(String.format("executing next request %1$s %2$s", nextAddress, nextMsg));
                vertx.eventBus().sendWithTimeout(nextAddress, nextMsg, TIMEOUT, nextHandler);
            } else {
                VertxAssert.testComplete();
            }
        }
    }

    private class DefaultChainableHandler extends ChainableHandler {

        private DefaultChainableHandler() {
            super(null, null, null);
        }

        private DefaultChainableHandler(String nextAddress, JsonObject nextMsg,
                                        AsyncResultHandler<Message<JsonObject>> nextHandler) {
            super(nextAddress, nextMsg, nextHandler);
        }

        @Override
        public void handle(AsyncResult<Message<JsonObject>> reply) {
            if (reply.succeeded()) {
                final JsonObject replyBody = reply.result().body();
                container.logger().info(replyBody);
            } else {
                container.logger().error(String.format("failed to process: %1$s", reply.cause().getMessage()),
                        reply.cause());
            }
            sendNext();
        }
    }

}
