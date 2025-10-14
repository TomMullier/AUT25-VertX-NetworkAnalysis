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
        logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 Starting MainVerticle..." + Colors.RESET);

        // Load configuration from JSON file
        JsonObject config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        logger.debug("[ MAIN VERTICLE ] Loaded configuration: " + config.encodePrettily());

        boolean store = config.getString("store", "false").equalsIgnoreCase("true");
        logger.info(Colors.YELLOW + "[ MAIN VERTICLE ] [ CONFIG ]      Store configuration: " + store + Colors.RESET);

        // Créer la liste de verticles à déployer
        List<AbstractVerticle> verticles = new ArrayList<>();
        verticles.add(new IngestionVerticle());
        verticles.add(new AnalyseVerticle());
        verticles.add(new FlowAggregatorVerticle());
        // verticles.add(new ApiVerticle());

        if (store) {
            verticles.add(new ClickHousePacketVerticle());
            verticles.add(new ClickHouseFlowsVerticle());
        } else {
            logger.info(Colors.YELLOW
                    + "[ MAIN VERTICLE ]                 Skipping ClickHouse verticles as per configuration."
                    + Colors.RESET);
        }

        // Déploiement séquentiel
        deployVerticlesSequentially(verticles, startPromise);
    }

    /**
     * Déploie les verticles un par un de manière séquentielle.
     */
    private void deployVerticlesSequentially(List<AbstractVerticle> verticles, Promise<Void> startPromise) {
        if (verticles.isEmpty()) {
            startPromise.complete();
            return;
        }

        AbstractVerticle verticle = verticles.remove(0);
        vertx.deployVerticle(verticle).onComplete(res -> {
            if (res.succeeded()) {
                String id = res.result();
                deploymentIds.add(id);
                logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 " + verticle.getClass().getSimpleName()
                        + " deployed successfully! id=" + id + Colors.RESET);
                // Passer au verticle suivant
                deployVerticlesSequentially(verticles, startPromise);
            } else {
                logger.error(Colors.RED + "[ MAIN VERTICLE ]                 Failed to deploy "
                        + verticle.getClass().getSimpleName() + ": " + res.cause() + Colors.RESET);
                startPromise.fail(res.cause());
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
        logger.info(Colors.RED + "[ MAIN VERTICLE ]                 Stopping MainVerticle..." + Colors.RESET);

        // Stopper tous les verticles explicitement
        if (deploymentIds.isEmpty()) {
            logger.info(Colors.RED + "[ MAIN VERTICLE ]                 No verticles to undeploy." + Colors.RESET);
            stopPromise.complete();
            return;
        }
    }
}
