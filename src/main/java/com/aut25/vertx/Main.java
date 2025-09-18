package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        logger.info("[ MAIN VERTICLE ] Starting MainVerticle...");

        /* ---------------------- Load configuration from file ---------------------- */
        JsonObject config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        logger.info("[ MAIN VERTICLE ] Loaded configuration: " + config.encodePrettily());

        /* ---------------------- Deploy the IngestionVerticle ---------------------- */
        vertx.deployVerticle(new IngestionVerticle(), res -> {
            if (res.succeeded()) {
                logger.info("[ MAIN VERTICLE ] IngestionVerticle deployed successfully!");
            } else {
                logger.error("[ MAIN VERTICLE ] Failed to deploy IngestionVerticle: " + res.cause());
            }
        });

        /* ---------------------- Deploy the AnalyseVerticle ---------------------- */
        vertx.deployVerticle(new AnalyseVerticle(), res -> {
            if (res.succeeded()) {
                logger.info("[ MAIN VERTICLE ] AnalyseVerticle deployed successfully!");
            } else {
                logger.error("[ MAIN VERTICLE ] Failed to deploy AnalyseVerticle: " + res.cause());
            }
        });

        /* ------------------------ Deploy the ApiVerticle ------------------------- */
        // vertx.deployVerticle(new ApiVerticle(), res -> {
        //     if (res.succeeded()) {
        //         logger.info("[ MAIN VERTICLE ] ApiVerticle deployed successfully!");
        //     } else {
        //         logger.error("[ MAIN VERTICLE ] Failed to deploy ApiVerticle: " + res.cause());
        //     }
        // });

        startPromise.complete(); // Signale que le MainVerticle est prêt
    }

    @Override
    public void stop() {
        logger.info("[ MAIN VERTICLE ] MainVerticle stopped!");

    }
}
