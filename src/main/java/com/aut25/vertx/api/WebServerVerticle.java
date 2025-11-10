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
import io.vertx.core.shareddata.SharedData;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import io.vertx.core.json.JsonArray;

import com.aut25.vertx.utils.Colors;
import com.aut25.vertx.Main;
import com.aut25.vertx.api.routes.*;

public class WebServerVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);
        private final Set<ServerWebSocket> clients = ConcurrentHashMap.newKeySet();
        private final Main mainVerticle;
        private JsonObject config;

        public WebServerVerticle(Main mainVerticle) {
                this.mainVerticle = mainVerticle;
        }

        @Override
        public void start(Promise<Void> startPromise) {
                logger.info(Colors.BLUE + "[ WEBSERVER ]                     Starting WebSocket and HTTP server"
                                + Colors.RESET);

                try {
                        LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");
                        config = new JsonObject(map);
                        int port = config.getInteger("http.port", 8888);

                        Router router = Router.router(vertx);
                        router.route().handler(BodyHandler.create());

                        // Routes API
                        new SettingsRoute(vertx, mainVerticle).mount(router);
                        new PcapRoute(vertx, mainVerticle).mount(router);
                        new NetworkRoute(vertx, mainVerticle).mount(router);
                        new UtilsRoute(vertx, mainVerticle).mount(router);                    
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

                        // vertx.eventBus().consumer("packets.data", msg -> {
                        //         if (!(msg.body() instanceof JsonObject))
                        //                 return;
                        //         JsonObject data = ((JsonObject) msg.body()).copy();
                        //         data.put("type", "packet");
                        //         broadcast(data);
                        // });

                        vertx.eventBus().consumer("currentFlows.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "currentFlow");
                                broadcast(data);
                        });

                        vertx.eventBus().consumer("malformedPackets.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "malformedPacket");
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

}
