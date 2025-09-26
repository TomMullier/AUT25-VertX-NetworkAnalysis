package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // List to track deployed verticle IDs
    private final List<String> deploymentIds = new ArrayList<>();

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        logger.info(Colors.GREEN + "[ MAIN VERTICLE ] Starting MainVerticle..." + Colors.RESET);

        // Load configuration from JSON file
        JsonObject config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        logger.debug("[ MAIN VERTICLE ] Loaded configuration: " + config.encodePrettily());

        boolean store = config.getString("store", "false").equalsIgnoreCase("true");
        logger.info("[ MAIN VERTICLE ] Store configuration: " + store);

        // Deploy verticles based on configuration
        deployAndTrack(new IngestionVerticle());
        deployAndTrack(new AnalyseVerticle());
        deployAndTrack(new FlowAggregatorVerticle());
        // deployAndTrack(new ApiVerticle());

        if (store) {
            deployAndTrack(new ClickHousePacketVerticle());
            deployAndTrack(new ClickHouseFlowsVerticle());
        }

        startPromise.complete();
    }

    /**
     * Deploy a verticle and track its deployment ID
     * 
     * @param verticle The verticle to deploy
     */
    private void deployAndTrack(AbstractVerticle verticle) {
        vertx.deployVerticle(verticle, res -> {
            if (res.succeeded()) {
                String id = res.result();
                deploymentIds.add(id);
                logger.info(Colors.GREEN + "[ MAIN VERTICLE ] " + verticle.getClass().getSimpleName()
                        + " deployed successfully! id=" + id + Colors.RESET);
            } else {
                logger.error(Colors.RED + "[ MAIN VERTICLE ] Failed to deploy "
                        + verticle.getClass().getSimpleName() + ": " + res.cause() + Colors.RESET);
            }
        });
    }

    /**
     * Stop all deployed verticles gracefully
     * 
     * @param stopPromise Promise to indicate when stopping is complete
     * @throws Exception if an error occurs during stopping
     */
    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        logger.info(Colors.RED + "[ MAIN VERTICLE ] Stopping MainVerticle..." + Colors.RESET);

        // Stopper tous les verticles explicitement
        if (deploymentIds.isEmpty()) {
            logger.info("[ MAIN VERTICLE ] No verticles to undeploy.");
            stopPromise.complete();
            return;
        }
    }
}
