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
        logger.info(Colors.GREEN + "[ MAIN VERTICLE ] Starting MainVerticle..." + Colors.RESET);

        /* ---------------------- Load configuration from file ---------------------- */
        JsonObject config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        logger.debug("[ MAIN VERTICLE ] Loaded configuration: " + config.encodePrettily());

        boolean store = config.getBoolean("store", false);
        logger.info("[ MAIN VERTICLE ] Store configuration: " + store);

        /* ---------------------- Deploy the IngestionVerticle ---------------------- */
        vertx.deployVerticle(new IngestionVerticle(), res -> {
            if (res.succeeded()) {
                logger.info(Colors.GREEN + "[ MAIN VERTICLE ] IngestionVerticle deployed successfully!" + Colors.RESET);
            } else {
                logger.error(Colors.RED + "[ MAIN VERTICLE ] Failed to deploy IngestionVerticle: " + res.cause()
                        + Colors.RESET);
            }
        });

        /* ---------------------- Deploy the AnalyseVerticle ---------------------- */
        vertx.deployVerticle(new AnalyseVerticle(), res -> {
            if (res.succeeded()) {
                logger.info(Colors.GREEN + "[ MAIN VERTICLE ] AnalyseVerticle deployed successfully!" + Colors.RESET);
            } else {
                logger.error(Colors.RED + "[ MAIN VERTICLE ] Failed to deploy AnalyseVerticle: " + res.cause()
                        + Colors.RESET);
            }
        });

        /* ----------------- Deploy the ClickHouseIngestionVerticle ----------------- */
        if (store) {
            vertx.deployVerticle(new ClickHouseIngestionVerticle(), res -> {
                if (res.succeeded()) {
                    logger.info(Colors.GREEN + "[ MAIN VERTICLE ] ClickHouseIngestionVerticle deployed successfully!"
                            + Colors.RESET);
                } else {
                    logger.error(Colors.RED + "[ MAIN VERTICLE ] Failed to deploy ClickHouseIngestionVerticle: "
                            + res.cause() + Colors.RESET);
                }
            });
        }

        /* ------------------------ Deploy the ApiVerticle ------------------------- */
        // vertx.deployVerticle(new ApiVerticle(), res -> {
        // if (res.succeeded()) {
        // logger.info(Colors.GREEN + "[ MAIN VERTICLE ] ApiVerticle deployed
        // successfully!" + Colors.RESET);
        // } else {
        // logger.error(Colors.RED + "[ MAIN VERTICLE ] Failed to deploy ApiVerticle: "
        // + res.cause() + Colors.RESET);
        // }
        // });

        startPromise.complete(); // Signale que le MainVerticle est prêt
    }

    @Override
    public void stop() {
        logger.info(Colors.RED + "[ MAIN VERTICLE ] MainVerticle stopped!" + Colors.RESET);

    }
}
