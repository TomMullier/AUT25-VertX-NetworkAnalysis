package com.aut25.vertx.api;

import io.netty.handler.codec.http.cors.CorsHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.*;
import io.vertx.core.http.HttpMethod;

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

                Router router = Router.router(vertx);

                router.route("/").handler(StaticHandler.create("web").setCachingEnabled(false));

                Router apiRouter = Router.router(vertx);
                // apiRouter.get("/flows").handler(this::getFlows);
                // apiRouter.get("/stats").handler(this::getStats);
                router.mountSubRouter("/api", apiRouter);

                HttpServer server = vertx.createHttpServer();

                server.requestHandler(router).listen(port, ar -> {
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