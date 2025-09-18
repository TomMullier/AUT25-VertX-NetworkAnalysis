package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(ApiVerticle.class);

        @Override
        public void start() throws Exception {
                // Lire depuis config.json
                JsonObject config = new JsonObject(
                                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
                int port = config.getInteger("http.port", 8888);
                logger.info("[ API VERTICLE ] API Verticle starting on port: " + port);

                HttpServer server = vertx.createHttpServer();

                server.requestHandler(req -> {
                        req.response().end("Vert.x API is running!");
                }).listen(port, ar -> {
                        if (ar.succeeded()) {
                                logger.info("[ API VERTICLE ] HTTP server started on port " + port);
                        } else {
                                logger.error("[ API VERTICLE ] Failed to start HTTP server: " + ar.cause());
                        }
                });

        }

        @Override
        public void stop() {
                logger.info("[ API VERTICLE ] API Verticle stopped!");
        }

}
