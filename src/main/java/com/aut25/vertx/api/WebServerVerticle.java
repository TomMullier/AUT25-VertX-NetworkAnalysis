package com.aut25.vertx.api;

import io.netty.handler.codec.http.cors.CorsHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.api.utils.FlowConsumerVerticle;
import com.aut25.vertx.utils.Colors;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.Promise;

public class WebServerVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);
        private final Set<ServerWebSocket> clients = ConcurrentHashMap.newKeySet();

        @Override
        public void start(Promise<Void> startPromise) {
                JsonObject config = config();
                int port = config.getInteger("http.port", 8888);

                Router router = Router.router(vertx);
                router.route("/*").handler(StaticHandler.create("webroot").setCachingEnabled(false));

                HttpServer server = vertx.createHttpServer();

                server.webSocketHandler(ws -> {
                        if (!"/".equals(ws.path())) {
                                ws.reject();
                                return;
                        }

                        logger.info("[WS] Nouveau client connecté");
                        clients.add(ws);

                        ws.closeHandler(v -> clients.remove(ws));
                });

                // 🔹 Ecoute des messages sur l'EventBus
                vertx.eventBus().consumer("flows.data", msg -> {
                        String data = msg.body().toString();
                        clients.forEach(ws -> {
                                if (!ws.isClosed())
                                        ws.writeTextMessage(data);
                        });
                });

                server.requestHandler(router)
                                .listen(port)
                                .onSuccess(s -> {
                                        logger.info(Colors.MAGENTA + "[ WEBSERVER ] Started on port " + port
                                                        + Colors.RESET);
                                        startPromise.complete();
                                })
                                .onFailure(startPromise::fail);
        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ WEBSERVER VERTICLE ]            WebServerVerticle stopped!" + Colors.RESET);
        }

}