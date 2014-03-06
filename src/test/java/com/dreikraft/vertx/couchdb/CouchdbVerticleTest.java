package com.dreikraft.vertx.couchdb;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
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

    private static final long TIMEOUT = 300000;
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

    /**
     * Queries all databases in the server.
     */
    @Test
    public void testAllDbs() {
        container.logger().info(String.format("sending message to address %1$s: %2$s",
                CouchdbVerticle.ADDRESS_ALL_DBS, new JsonObject()));
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

    /**
     * Queries all documents in a database.
     */
    @Test
    public void testAllDbDocs() {
        final JsonObject message = new JsonObject().putArray("params", new JsonArray("[{\"include_docs\":true}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_ALL_DOCS, "dummy");
        container.logger().info(String.format("sending message to address %1$s: %2$s", address, message));
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

    /**
     * Queries a couchdb view.
     */
    @Test
    public void testQueryView() {
        final JsonObject message = new JsonObject().putArray("params",
                new JsonArray("[{\"include_docs\":true}, {\"reduce\":false}]"));
        final String address = String.format(CouchdbVerticle.ADDRESS_VIEW, "dummy", "_design/dummy", "all");
        container.logger().info(String.format("sending message to address %1$s: %2$s", address, message));
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

    /**
     * Retrieves a document from couchdb.
     */
    @Test
    public void testGetDoc() {
        final JsonObject message = new JsonObject().putString("id", "dummy1");
        final String address = String.format(CouchdbVerticle.ADDRESS_DB, "dummy");
        container.logger().info(String.format("sending message to address %1$s: %2$s", address, message));
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
                    new CreateTestDbHandler());
        }

        /**
         * Triggers database handler registration after the dummy database has been created.
         */
        private class CreateTestDbHandler implements AsyncResultHandler<Message<JsonObject>> {

            /**
             * Handles database creation events.
             *
             * @param reply the database creation result
             */
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
                    container.logger().info(String.format("sending message to address %1$s: %2$s",
                            registerDbHandlersAddress, registerDummyHandlersMessage.toString()));
                    vertx.eventBus().sendWithTimeout(registerDbHandlersAddress,
                            registerDummyHandlersMessage, TIMEOUT,
                            new RegisterTestDbResultHandler(registerDbHandlersAddress, registerDummyHandlersMessage));

                } else {
                    container.logger().error(String.format("failed to perform %1$s: %2$s",
                            CouchdbVerticle.ADDRESS_SERVER, reply.cause().getMessage()), reply.cause());
                }
            }

            /**
             * Bulk loads documents and views into the dummy database after registering the db handlers.
             */
            private class RegisterTestDbResultHandler implements AsyncResultHandler<Message<JsonObject>> {
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
                                new ResisterTestBulkLoadResultHandler(bulkLoadAddress, registerDbHandlersReply));
                    } else {
                        container.logger().error(String.format("failed to perform %1$s: %2$s",
                                registerDbHandlersAddress,
                                registerDbHandlersReply.cause().getMessage()),
                                registerDbHandlersReply.cause());
                    }
                }

                /**
                 * Handles buld load results and triggers reregistry of db/view handlers
                 */
                private class ResisterTestBulkLoadResultHandler implements AsyncResultHandler<Message<JsonArray>> {
                    private final String bulkLoadAddress;
                    private final AsyncResult<Message<JsonObject>> registerDbHandlersReply;

                    /**
                     * Creates the handler.
                     * @param bulkLoadAddress the address of the bulk load handler
                     * @param registerDbHandlersReply the result
                     */
                    public ResisterTestBulkLoadResultHandler(String bulkLoadAddress,
                                                             AsyncResult<Message<JsonObject>> registerDbHandlersReply) {
                        this.bulkLoadAddress = bulkLoadAddress;
                        this.registerDbHandlersReply = registerDbHandlersReply;
                    }

                    /**
                     * Handles bulk load result events and triggers registration of database view handlers
                     * @param bulkLoadReply the result
                     */
                    @Override
                    public void handle(final AsyncResult<Message<JsonArray>> bulkLoadReply) {
                        if (bulkLoadReply.succeeded()) {
                            container.logger().info(String.format(String.format("result for %1$s:  %2$s",
                                    bulkLoadAddress, bulkLoadReply.result().body())));

                            // reregister view handlers after schema import
                            container.logger().info(String.format("sending message to address %1$s: %2$s",
                                    registerDbHandlersAddress, registerDummyHandlersMessage.toString()));
                            vertx.eventBus().sendWithTimeout(registerDbHandlersAddress, registerDummyHandlersMessage,
                                    TIMEOUT, new AsyncResultHandler<Message<JsonObject>>() {

                                @Override
                                public void handle(final AsyncResult<Message<JsonObject>> reregisterReply) {
                                    if (reregisterReply.succeeded()) {
                                        container.logger().info(String.format(String.format(
                                                "result for %1$s:  %2$s", registerDbHandlersAddress,
                                                reregisterReply.result().body())));
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
                }
            }
        }
    }
}
