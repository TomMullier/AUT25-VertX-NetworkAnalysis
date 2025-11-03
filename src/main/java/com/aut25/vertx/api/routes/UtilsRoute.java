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

public class UtilsRoute {
        /* -------------------------------- Variables ------------------------------- */
        private static final Logger logger = LoggerFactory.getLogger(UtilsRoute.class);

        private final Vertx vertx;
        private final Main mainVerticle;

        /* ------------------------------- Constructor ------------------------------ */
        public UtilsRoute(Vertx vertx, Main mainVerticle) {
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
                router.get("/api/checkFileExists").handler(this::checkIfFileExists);
        }

        /* --------------------------------- Methods -------------------------------- */
        /**
         * Handle check if file exists request.
         * 
         * @param ctx The routing context.
         */
        private void checkIfFileExists(RoutingContext ctx) {
                String fileName = ctx.request().getParam("file");

                if (fileName == null || fileName.isEmpty()) {
                        ctx.response()
                                        .setStatusCode(400)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new io.vertx.core.json.JsonObject()
                                                        .put("error", "Missing 'file' parameter")
                                                        .encode());
                        return;
                }

                String pcapDirPath = "src/main/resources/data";
                java.nio.file.Path filePath = java.nio.file.Paths.get(pcapDirPath, fileName);

                boolean exists = java.nio.file.Files.exists(filePath);

                ctx.response()
                                .putHeader("Content-Type", "application/json")
                                .end(new io.vertx.core.json.JsonObject().put("exists", exists)
                                                .encode());

        }
}
