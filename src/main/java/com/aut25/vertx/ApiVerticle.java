package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ApiVerticle extends AbstractVerticle {

        @Override
        public void start() throws Exception {
                // Lire depuis config.json
                JsonObject config = new JsonObject(
                                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
                int port = config.getInteger("http.port", 8888);
                System.out.println("[ API VERTICLE ] API Verticle starting on port: " + port);

                HttpServer server = vertx.createHttpServer();

                server.requestHandler(req -> {
                        req.response().end("Vert.x API is running!");
                }).listen(port, ar -> {
                        if (ar.succeeded()) {
                                System.out.println("[ API VERTICLE ] HTTP server started on port " + port);
                        } else {
                                System.err.println("[ API VERTICLE ] Failed to start HTTP server: " + ar.cause());
                        }
                });
        }
}
