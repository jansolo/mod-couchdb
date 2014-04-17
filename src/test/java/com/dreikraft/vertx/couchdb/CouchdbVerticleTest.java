package com.dreikraft.vertx.couchdb;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Tests the CouchdbVerticle.
 *
 * @author jansolo
 */
public class CouchdbVerticleTest extends TestVerticle {

    private static final String DB_NAME = "dummy";

    /**
     * Initializes the module test. Starts vert.x and creates a dummy couchdb database. Couchdb needs to up and
     * running (and configured) to perform the tests successfully.
     */
    @Override
    public void start() {
        super.initialize();

        final StringBuilder confBody = new StringBuilder();
        final URL confURL = getClass().getResource("/conf.json");
        try (LineNumberReader lnr = new LineNumberReader(new FileReader(new File(confURL.toURI())))) {
            String line;
            while ((line = lnr.readLine()) != null) {
                confBody.append(line).append("\n");
            }
            final JsonObject config = new JsonObject(confBody.toString());
            container.logger().info(String.format("starting %1$s tests ...", CouchdbVerticleTest.class.getName()));
            container.deployModule(System.getProperty("vertx.modulename"), config, new CouchdbTestInitHandler());
        } catch (URISyntaxException | IOException | NullPointerException ex) {
            container.logger().error(String.format("failed to read config %1$s: %2$s", confURL, ex.getMessage()), ex);
        }
    }

    /**
     * Deletes the test database and completes the test.
     */
    private void shutdown() {
        final JsonObject deleteDbMsg = new JsonObject().putString("method", "DELETE").putString("db", DB_NAME);
        container.logger().info(String.format("sending message to address %1$s: %2$s",
                CouchdbVerticle.ADDRESS_SERVER, deleteDbMsg));
        vertx.eventBus().send(CouchdbVerticle.ADDRESS_SERVER, deleteDbMsg, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                try {
                    container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                            CouchdbVerticle.ADDRESS_SERVER, reply.body())));
                } catch (RuntimeException ex) {
                    container.logger().error(String.format("failed to perform %1$s: %2$s",
                            CouchdbVerticle.ADDRESS_SERVER, ex.getMessage()), ex);
                }
                VertxAssert.testComplete();
            }
        });
    }

    /**
     * Queries all databases in the server.
     */
    @Test
    public void testAllDbs() {
        container.logger().info(String.format("sending message to address %1$s: %2$s",
                CouchdbVerticle.ADDRESS_ALL_DBS, new JsonObject()));
        vertx.eventBus().send(CouchdbVerticle.ADDRESS_ALL_DBS, new JsonObject(),
                new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(final Message<JsonObject> reply) {
                        try {
                            VertxAssert.assertEquals("ok", reply.body().getString("status"));
                            final JsonArray dbs = reply.body().getArray("body");
                            container.logger().info(String.format("/_all_dbs: %1$s", dbs.encode()));
                            VertxAssert.assertTrue("no dbs found", dbs.size() > 0);
                        } catch (RuntimeException ex) {
                            container.logger().error("failed to perform /_all_dbs", ex);
                            VertxAssert.fail(ex.getMessage());
                        }
                        shutdown();
                    }
                }
        );
    }

    /**
     * Queries all documents in a database.
     */
    @Test
    public void testAllDbDocs() {
        final JsonObject message = new JsonObject().putArray("params", new JsonArray("[{\"include_docs\":true}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_ALL_DOCS, "dummy");
        container.logger().info(String.format("sending message to address %1$s: %2$s", address, message));
        vertx.eventBus().send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                try {
                    VertxAssert.assertEquals("ok", reply.body().getString("status"));
                    final JsonObject docs = reply.body().getObject("body");
                    container.logger().info(String.format("%1$s: %2$s", address, docs.encode()));
                    VertxAssert.assertTrue("no docs found", docs.size() > 0);
                } catch (RuntimeException ex) {
                    container.logger().error(String.format("failed to perform %1$s", address),
                            ex);
                    VertxAssert.fail(ex.getMessage());
                }
                shutdown();
            }
        });
    }

    /**
     * Queries a couchdb view.
     */
    @Test
    public void testQueryView() {
        final JsonObject message = new JsonObject().putArray("params",
                new JsonArray("[{\"include_docs\":true}, {\"reduce\":false}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_VIEW, "dummy", "_design/dummy", "all");
        container.logger().info(String.format("sending message to address %1$s: %2$s", address, message));
        vertx.eventBus().send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                try {
                    VertxAssert.assertEquals("ok", reply.body().getString("status"));
                    final JsonObject docs = reply.body().getObject("body");
                    container.logger().info(String.format("%1$s: %2$s", address, docs.encode()));
                    VertxAssert.assertTrue("no docs found", docs.size() > 0);
                } catch (RuntimeException ex) {
                    container.logger().error(String.format("failed to perform %1$s: %2$s", address,
                            ex.getMessage()), ex);
                    VertxAssert.fail(ex.getMessage());
                }
                shutdown();
            }
        });
    }

    /**
     * Retrieves a document from couchdb.
     */
    @Test
    public void testGetDoc() {
        final JsonObject message = new JsonObject().putString("id", "dummy1");
        final String address = String.format(CouchdbVerticle.ADDRESS_DB, "dummy");
        container.logger().info(String.format("sending message to address %1$s: %2$s", address, message));
        vertx.eventBus().send(address, message, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(final Message<JsonObject> reply) {
                try {
                    VertxAssert.assertEquals("ok", reply.body().getString("status"));
                    final JsonObject docs = reply.body().getObject("body");
                    container.logger().info(String.format("%1$s: %2$s", "", docs.encode()));
                    VertxAssert.assertTrue("no docs found", docs.size() > 0);
                } catch (RuntimeException ex) {
                    container.logger().error(String.format("failed to perform %1$s: %2$s", address,
                            ex.getMessage()), ex);
                    VertxAssert.fail(ex.getMessage());
                }
                shutdown();
            }
        });
    }

    /**
     * Creates a document.
     */
    @Test
    public void testCreateDoc() {

        final String serverAddress = CouchdbVerticle.ADDRESS_SERVER;
        final JsonObject createDocMsg = new JsonObject().putString("method", "POST").putString("db",
                DB_NAME).putObject("body", new JsonObject().putString("dummy", "dummy"));
        container.logger().info(String.format("sending message to address %1$s: %2$s",
                CouchdbVerticle.ADDRESS_SERVER, createDocMsg));
        vertx.eventBus().send(serverAddress, createDocMsg,
                new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(final Message<JsonObject> reply) {
                        try {
                            VertxAssert.assertEquals("ok", reply.body().getString("status"));
                            container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                    serverAddress, reply.body())));
                            VertxAssert.assertTrue("not ok", reply.body().getObject("body").getBoolean("ok"));
                        } catch (RuntimeException ex) {
                            container.logger().error(String.format("failed to perform %1$s: %2$s",
                                    serverAddress, ex.getMessage()), ex);
                        }
                        shutdown();
                    }
                }
        );
    }

    /**
     * Initializes a couchdb database for testing.
     */
    private class CouchdbTestInitHandler implements AsyncResultHandler<String> {

        /**
         * Waits for vert.x to be started and triggers dummy database creation.
         *
         * @param asyncResult
         */
        @Override
        public void handle(final AsyncResult<String> asyncResult) {
            container.logger().info(String.format("successfully started %1$s tests",
                    CouchdbVerticle.class.getName()));
            // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
            if (asyncResult.failed()) {
                container.logger().error(String.format("failed to start %1$s tests",
                        CouchdbVerticle.class.getName()), asyncResult.cause());
            }
            VertxAssert.assertTrue(asyncResult.succeeded());
            VertxAssert.assertNotNull("deploymentID should not be null", asyncResult);

            if (asyncResult.succeeded()) {
                // create a test database
                final JsonObject createDbMsg = new JsonObject().putString("method", "PUT").putString("db", DB_NAME);
                container.logger().info(String.format("sending message to address %1$s: %2$s",
                        CouchdbVerticle.ADDRESS_SERVER, createDbMsg));
                vertx.eventBus().send(CouchdbVerticle.ADDRESS_SERVER, createDbMsg,
                        new CreateTestDbHandler());
            }
        }

        /**
         * Triggers database handler registration after the dummy database has been created.
         */
        private class CreateTestDbHandler implements Handler<Message<JsonObject>> {

            /**
             * Handles database creation events.
             *
             * @param reply the database creation result
             */
            @Override
            public void handle(final Message<JsonObject> reply) {
                try {
                    container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                            CouchdbVerticle.ADDRESS_SERVER, reply.body())));
                    if ("ok".equals(reply.body().getString("status"))) {
                        // register db handlers for dummy db
                        final String registerDbHandlersAddress = CouchdbVerticle
                                .ADDRESS_REFLECT;
                        final JsonObject registerDummyHandlersMessage = new
                                JsonObject().putString("db", DB_NAME);
                        container.logger().info(String.format("sending message to address %1$s: %2$s",
                                registerDbHandlersAddress, registerDummyHandlersMessage.toString()));
                        vertx.eventBus().send(registerDbHandlersAddress, registerDummyHandlersMessage,
                                new RegisterTestDbResultHandler(registerDbHandlersAddress, registerDummyHandlersMessage));
                    } else {
                        container.logger().error(String.format("failed to perform %1$s: %2$s",
                                CouchdbVerticle.ADDRESS_SERVER, reply.body().getString("message")));
                        shutdown();
                    }
                } catch (RuntimeException ex) {
                    container.logger().error(String.format("failed to perform %1$s: %2$s",
                            CouchdbVerticle.ADDRESS_SERVER, ex.getMessage()), ex);
                    shutdown();
                }
            }

            /**
             * Bulk loads documents and views into the dummy database after registering the db handlers.
             */
            private class RegisterTestDbResultHandler implements Handler<Message<JsonObject>> {
                private final String registerDbHandlersAddress;
                private final JsonObject registerDummyHandlersMessage;

                /**
                 * Creates the handlers
                 *
                 * @param registerDbHandlersAddress    the register db handler address
                 * @param registerDummyHandlersMessage the register db message
                 */
                public RegisterTestDbResultHandler(String registerDbHandlersAddress,
                                                   JsonObject registerDummyHandlersMessage) {
                    this.registerDbHandlersAddress = registerDbHandlersAddress;
                    this.registerDummyHandlersMessage = registerDummyHandlersMessage;
                }

                /**
                 * Handles db handler registrion event.
                 *
                 * @param registerDbHandlersReply the result
                 */
                @Override
                public void handle(final Message<JsonObject> registerDbHandlersReply) {
                    try {
                        container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                registerDbHandlersAddress, registerDbHandlersReply.body())));
                        if ("ok".equals(registerDbHandlersReply.body().getString("status"))) {

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
                            vertx.eventBus().send(bulkLoadAddress, dummyDbMsg,
                                    new ResisterTestBulkLoadResultHandler(bulkLoadAddress, registerDbHandlersReply));
                        } else {
                            container.logger().error(String.format("failed to perform %1$s: %2$s",
                                    registerDbHandlersAddress, registerDbHandlersReply.body().getString
                                            ("message")
                            ));
                            shutdown();
                        }
                    } catch (RuntimeException ex) {
                        container.logger().error(String.format("failed to perform %1$s: %2$s",
                                registerDbHandlersAddress, ex.getMessage()), ex);
                        shutdown();
                    }
                }

                /**
                 * Handles buld load results and triggers reregistry of db/view handlers
                 */
                private class ResisterTestBulkLoadResultHandler implements Handler<Message<JsonObject>> {
                    private final String bulkLoadAddress;
                    private final Message<JsonObject> registerDbHandlersReply;

                    /**
                     * Creates the handler.
                     *
                     * @param bulkLoadAddress         the address of the bulk load handler
                     * @param registerDbHandlersReply the result
                     */
                    public ResisterTestBulkLoadResultHandler(String bulkLoadAddress,
                                                             Message<JsonObject> registerDbHandlersReply) {
                        this.bulkLoadAddress = bulkLoadAddress;
                        this.registerDbHandlersReply = registerDbHandlersReply;
                    }

                    /**
                     * Handles bulk load result events and triggers registration of database view handlers
                     *
                     * @param bulkLoadReply the result
                     */
                    @Override
                    public void handle(final Message<JsonObject> bulkLoadReply) {
                        try {
                            container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                    bulkLoadAddress, bulkLoadReply.body())));

                            if ("ok".equals(bulkLoadReply.body().getString("status"))) {
                                // reregister view handlers after schema import
                                container.logger().info(String.format("sending message to address %1$s: %2$s",
                                        registerDbHandlersAddress, registerDummyHandlersMessage.toString()));
                                vertx.eventBus().send(registerDbHandlersAddress, registerDummyHandlersMessage,
                                        new Handler<Message<JsonObject>>() {

                                            @Override
                                            public void handle(final Message<JsonObject> reregisterReply) {
                                                try {
                                                    if ("ok".equals(reregisterReply.body().getString("status"))) {
                                                        container.logger().info(String.format(String.format(
                                                                "result for %1$s:  %2$s", registerDbHandlersAddress,
                                                                reregisterReply.body())));
                                                        // start the tests
                                                        startTests();
                                                    } else {
                                                        container.logger().error(String.format("failed to perform %1$s: %2$s",
                                                                registerDbHandlersAddress,
                                                                reregisterReply.body().getString("message")));
                                                        shutdown();
                                                    }
                                                } catch (RuntimeException ex) {
                                                    container.logger().error(String.format("failed to perform %1$s: %2$s",
                                                            registerDbHandlersAddress, ex.getMessage()), ex);
                                                    shutdown();
                                                }
                                            }
                                        }
                                );
                            } else {
                                container.logger().error(String.format("failed to perform %1$s: %2$s",
                                        bulkLoadAddress, bulkLoadReply.body().getString("message")));
                                shutdown();
                            }
                        } catch (RuntimeException ex) {
                            container.logger().error(String.format("failed to perform %1$s: %2$s",
                                    bulkLoadAddress, ex.getMessage()), ex);
                            shutdown();
                        }
                    }
                }
            }
        }
    }
}
