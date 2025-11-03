package com.aut25.vertx.api;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;

import com.aut25.vertx.Main;

public class WebServerVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);
        private final Set<ServerWebSocket> clients = ConcurrentHashMap.newKeySet();
        private final Main mainVerticle;

        public WebServerVerticle(Main mainVerticle) {
                this.mainVerticle = mainVerticle;
        }

        @Override
        public void start(Promise<Void> startPromise) {
                logger.info(Colors.BLUE + "[ WEBSERVER ]                     Starting WebSocket and HTTP server"
                                + Colors.RESET);

                try {
                        JsonObject config = config();
                        int port = config.getInteger("http.port", 8888);

                        Router router = Router.router(vertx);
                        router.route().handler(BodyHandler.create());
                        router.post("/api/settings").handler(this::handleSettingsUpdate);
                        router.get("/api/settings").handler(ctx -> {
                                LocalMap<String, Object> settingsMap = vertx.sharedData().getLocalMap("settings");
                                JsonObject settings = (JsonObject) settingsMap.get("config");
                                if (settings == null) {
                                        settings = new JsonObject();
                                }
                                ctx.response()
                                                .putHeader("Content-Type", "application/json")
                                                .end(settings.encode());
                        });
                        router.get("/api/getIngestionMethod").handler(ctx -> {
                                LocalMap<String, Object> config_ = vertx.sharedData().getLocalMap("config");
                                String method = (String) config_.getOrDefault("ingestionMethod", "none");

                                ctx.response()
                                                .putHeader("Content-Type", "application/json")
                                                .end(new JsonObject().put("ingestionMethod", method).encode());
                        });

                        router.route("/*").handler(StaticHandler.create("webroot").setCachingEnabled(false));

                        HttpServer server = vertx.createHttpServer();

                        // Gestion WebSocket
                        server.webSocketHandler(ws -> {
                                if (!"/".equals(ws.path())) {
                                        ws.reject();
                                        return;
                                }

                                logger.info("[WS]                              New client connected: {}",
                                                ws.remoteAddress());
                                clients.add(ws);

                                ws.closeHandler(v -> {
                                        logger.info("[WS]                              Client disconnected: {}",
                                                        ws.remoteAddress());
                                        clients.remove(ws);
                                });

                                ws.exceptionHandler(err -> {
                                        logger.error("[WS]                              Error on connection {}: {}",
                                                        ws.remoteAddress(), err.getMessage());
                                        clients.remove(ws);
                                });
                        });

                        vertx.eventBus().consumer("flows.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "flow");
                                broadcast(data);
                        });

                        vertx.eventBus().consumer("packets.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "packet");
                                broadcast(data);
                        });

                        server.requestHandler(router)
                                        .listen(port)
                                        .onSuccess(s -> {
                                                logger.info(Colors.MAGENTA
                                                                + "[ WEBSERVER ]                     Started on port "
                                                                + port + Colors.RESET);
                                                if (!startPromise.future().isComplete())
                                                        startPromise.complete();
                                        })
                                        .onFailure(err -> {
                                                logger.error("[ WEBSERVER ]                     Failed to start: ",
                                                                err);
                                                if (!startPromise.future().isComplete())
                                                        startPromise.fail(err);
                                        });

                } catch (Exception e) {
                        logger.error("[ WEBSERVER ]                     Critical exception during start()", e);
                        if (!startPromise.future().isComplete())
                                startPromise.fail(e);
                }
        }

        /**
         * Diffuse un message JSON à tous les clients WebSocket connectés.
         */
        private void broadcast(JsonObject data) {
                String message = data.encode();
                clients.removeIf(ws -> ws == null || ws.isClosed());
                for (ServerWebSocket ws : clients) {
                        try {
                                ws.writeTextMessage(message);
                        } catch (Exception e) {
                                logger.warn("[WS]                              Unable to send to {}: {}",
                                                ws.remoteAddress(), e.getMessage());
                                clients.remove(ws);
                        }
                }
        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ WEBSERVER ]                     WebSocket and HTTP server stopped!"
                                + Colors.RESET);
                clients.forEach(ws -> {
                        if (!ws.isClosed())
                                ws.close();
                });
                clients.clear();
        }

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

                // Récupère les autres paramètres avec des valeurs par défaut
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
                // Stocke ces valeurs dans le sharedData (pour être accessibles aux autres
                // Verticles)
                JsonObject settings = new JsonObject()
                                .put("ingestionMethod", ingestionMethod)
                                .put("FLOW_INACTIVITY_TIMEOUT_MS_TCP", tcpTimeout)
                                .put("FLOW_INACTIVITY_TIMEOUT_MS_UDP", udpTimeout)
                                .put("FLOW_INACTIVITY_TIMEOUT_MS_OTHER", otherTimeout)
                                .put("FLOW_MAX_AGE_MS_TCP", tcpMaxAge)
                                .put("FLOW_MAX_AGE_MS_UDP", udpMaxAge)
                                .put("FLOW_MAX_AGE_MS_OTHER", otherMaxAge);

                vertx.sharedData().getLocalMap("settings").put("config", settings);

                // redeploy ingestion
                if (mainVerticle != null) {
                        mainVerticle.redeployIngestionVerticle(ingestionMethod);
                        mainVerticle.redeployFlowAggregatorVerticle(ingestionMethod);
                }
                ctx.response().setStatusCode(200).end();
        }

}
