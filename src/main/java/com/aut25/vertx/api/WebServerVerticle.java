package com.aut25.vertx.api;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;

public class WebServerVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);

        @Override
        public void start() throws Exception {
                // Lire depuis config()
                JsonObject config;
                try {
                        config = config();
                        logger.debug("[ WEBSERVER VERTICLE ]            Configuration loaded: "
                                        + config.encodePrettily());
                } catch (Exception e) {
                        logger.error("[ WEBSERVER VERTICLE ]            Failed to load configuration: "
                                        + e.getMessage());
                        return;
                }
                int port = config.getInteger("http.port", 8888);
                logger.info(Colors.MAGENTA + "[ WEBSERVER VERTICLE ]            Starting HTTP server on port " + port
                                + Colors.RESET);

                HttpServer server = vertx.createHttpServer();

                server.requestHandler(req -> {
                        req.response().end("Vert.x API is running!");
                }).listen(port, ar -> {
                        if (ar.succeeded()) {
                                logger.info(Colors.MAGENTA
                                                + "[ WEBSERVER VERTICLE ]            HTTP server started on port "
                                                + port + Colors.RESET);
                        } else {
                                logger.error("[ WEBSERVER VERTICLE ]            Failed to start HTTP server: "
                                                + ar.cause());
                        }
                });

        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ WEBSERVER VERTICLE ]            WebServerVerticle stopped!" + Colors.RESET);
        }

}