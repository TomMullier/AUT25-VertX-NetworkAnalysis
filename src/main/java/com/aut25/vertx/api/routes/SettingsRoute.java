package com.aut25.vertx.api.routes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.Main;
import com.aut25.vertx.api.WebServerVerticle;
import com.aut25.vertx.utils.Colors;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class SettingsRoute {
        /* -------------------------------- Variables ------------------------------- */
        private static final Logger logger = LoggerFactory.getLogger(SettingsRoute.class);

        private final Vertx vertx;
        private final Main mainVerticle;

        /* ------------------------------- Constructor ------------------------------ */
        public SettingsRoute(Vertx vertx, Main mainVerticle) {
                this.vertx = vertx;
                this.mainVerticle = mainVerticle;
        }

        /* --------------------------------- Routes --------------------------------- */
        /**
         * Mount the network routes to the given router.
         * 
         * @param router The Vert.x router to mount the routes on.
         */
        public void mount(Router router) {
                router.post("/api/settings").handler(this::handleSettingsUpdate);
                router.get("/api/settings").handler(this::getSettings);
                router.get("/api/getIngestionMethod").handler(this::getIngestionMethod);
        }

        /* --------------------------------- Methods -------------------------------- */

        /**
         * Handle get ingestion method request.
         * 
         * @param ctx The routing context containing the request data.
         */
        private void getIngestionMethod(RoutingContext ctx) {
                LocalMap<String, Object> config_ = vertx.sharedData().getLocalMap("config");
                String method = (String) config_.getOrDefault("ingestionMethod", "none");

                ctx.response()
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject().put("ingestionMethod", method).encode());
        }

        /**
         * Handle get settings request.
         * 
         * @param ctx The routing context containing the request data.
         */
        private void getSettings(RoutingContext ctx) {
                SharedData sharedData = vertx.sharedData();
                JsonObject settings = (JsonObject) sharedData.getLocalMap("config");
                if (settings == null) {
                        settings = new JsonObject();
                }
                ctx.response()
                                .putHeader("Content-Type", "application/json")
                                .end(settings.encode());
        }

        /**
         * Handle settings update request.
         * 
         * @param ctx The routing context containing the request data.
         */
        private void handleSettingsUpdate(RoutingContext ctx) {
                JsonObject body = ctx.body().asJsonObject();
                logger.info("[ WEBSERVER ]                     Received settings update: {}",
                                body != null ? body.encode() : "null");

                if (body == null) {
                        ctx.response()
                                        .setStatusCode(400)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject().put("error", "Invalid JSON").encode());
                        return;
                }
                // Validate ingestion method
                if (!body.containsKey("ingestionMethod")) {
                        ctx.response()
                                        .setStatusCode(400)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject().put("error", "Missing 'ingestionMethod' field").encode());
                        return;
                }
                String ingestionMethod = body.getString("ingestionMethod");
                if (!ingestionMethod.equals("pcap") &&
                                !ingestionMethod.equals("realtime")) {
                        ctx.response()
                                        .setStatusCode(400)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject().put("error", "Invalid 'ingestionMethod' value").encode());
                        return;
                }

                // Validate flow timeout and max age fields
                if (!body.containsKey("FLOW_INACTIVITY_TIMEOUT_MS_TCP") ||
                                !body.containsKey("FLOW_INACTIVITY_TIMEOUT_MS_UDP") ||
                                !body.containsKey("FLOW_INACTIVITY_TIMEOUT_MS_OTHER") ||
                                !body.containsKey("FLOW_MAX_AGE_MS_TCP") ||
                                !body.containsKey("FLOW_MAX_AGE_MS_UDP") ||
                                !body.containsKey("FLOW_MAX_AGE_MS_OTHER")) {
                        ctx.response()
                                        .setStatusCode(400)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject().put("error", "Missing flow timeout or max age fields")
                                                        .encode());
                        return;
                }
                long tcpTimeout = body.getLong("FLOW_INACTIVITY_TIMEOUT_MS_TCP", 300000L);
                long udpTimeout = body.getLong("FLOW_INACTIVITY_TIMEOUT_MS_UDP", 300000L);
                long otherTimeout = body.getLong("FLOW_INACTIVITY_TIMEOUT_MS_OTHER", 300000L);
                long tcpMaxAge = body.getLong("FLOW_MAX_AGE_MS_TCP", 3600000L);
                long udpMaxAge = body.getLong("FLOW_MAX_AGE_MS_UDP", 3600000L);
                long otherMaxAge = body.getLong("FLOW_MAX_AGE_MS_OTHER", 3600000L);

                // retrieve pcap file if ingestion method is pcap
                String activePcapFile = null;
                if (ingestionMethod.equals("pcap")) {
                        if (!body.containsKey("pcapFilePath")) {
                                ctx.response()
                                                .setStatusCode(400)
                                                .putHeader("Content-Type", "application/json")
                                                .end(new JsonObject().put("error", "Missing 'pcapFilePath' field")
                                                                .encode());
                                return;
                        }
                        activePcapFile = "src/main/resources/data/" + body.getString("pcapFilePath");
                }

                // Update settings in shared data
                JsonObject settings = new JsonObject()
                                .put("ingestionMethod", ingestionMethod)
                                .put("FLOW_INACTIVITY_TIMEOUT_MS_TCP", tcpTimeout)
                                .put("FLOW_INACTIVITY_TIMEOUT_MS_UDP", udpTimeout)
                                .put("FLOW_INACTIVITY_TIMEOUT_MS_OTHER", otherTimeout)
                                .put("FLOW_MAX_AGE_MS_TCP", tcpMaxAge)
                                .put("FLOW_MAX_AGE_MS_UDP", udpMaxAge)
                                .put("FLOW_MAX_AGE_MS_OTHER", otherMaxAge)
                                .put("pcap", new JsonObject().put("file-path",
                                                ingestionMethod.equals("pcap") ? activePcapFile : ""));

                vertx.sharedData().getLocalMap("config").putAll(settings.getMap());

                logger.info(Colors.BLUE + "[ WEBSERVER ]                     Settings updated: "
                                + vertx.sharedData().getLocalMap("config").toString() + Colors.RESET);

                ctx.response().setStatusCode(200).end();

                // Redeploy ingestion and flow aggregator verticles to apply new settings
                vertx.executeBlocking(promise -> {
                        if (mainVerticle != null) {
                                mainVerticle.redeployIngestionVerticle();
                                mainVerticle.redeployFlowAggregatorVerticle();
                        }
                        promise.complete();
                }, res -> {
                });
        }

}
