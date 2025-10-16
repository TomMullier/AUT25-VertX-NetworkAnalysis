package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import static java.lang.Thread.sleep;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Scanner;
import java.io.BufferedWriter;

public class Main extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // List to track deployed verticle IDs
    private final List<String> deploymentIds = new ArrayList<>();
    private JsonObject config;

    private JsonObject originalConfig = new JsonObject()
            .put("http.port", 8080)
            .put("store", "true")
            .put("mode", "menu")
            .put("json", new JsonObject()
                    .put("file-path", "src/main/resources/data/network-data.json")
                    .put("ingestion-interval-ms", 1000))
            .put("pcap", new JsonObject()
                    .put("file-path", "src/main/resources/data/benign+slowloris_net_packets.pcap"))
            .put("realtime_test", new JsonObject()
                    .put("interface", "ens160"))
            .put("realtime", new JsonObject());

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 Starting MainVerticle..." + Colors.RESET);

        // Load configuration from JSON file
        config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        logger.debug("[ MAIN VERTICLE ] Loaded configuration: " + config.encodePrettily());

        // Check the mode in the configuration
        String mode = config.getString("mode", "menu");

        if ("menu".equalsIgnoreCase(mode)) {
            // Open a menu to ask the user to choose the ingestion method
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 Choose ingestion method:");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 1. JSON (provide file path)");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 2. PCAP (provide file path)");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 3. Realtime" + Colors.RESET);

            int choice = -1;
            Scanner scanner = new Scanner(System.in);
            while (choice < 1 || choice > 3) {
                logger.info(
                        Colors.MAGENTA + "[ MAIN VERTICLE ]                 Enter your choice (1-3): " + Colors.RESET);
                try {
                    choice = Integer.parseInt(scanner.nextLine());
                } catch (NumberFormatException e) {
                    logger.error("Invalid input. Please enter a number between 1 and 3.");
                }
            }

            switch (choice) {
                case 1:
                    logger.info(
                            Colors.MAGENTA + "[ MAIN VERTICLE ] Enter JSON file path: " + Colors.RESET);
                    String jsonPath = scanner.nextLine();
                    if (jsonPath.trim().isEmpty()) {
                        jsonPath = config.getJsonObject("json").getString("file-path",
                                "src/main/resources/data/network-data.json");
                        logger.info(Colors.YELLOW
                                + "[ MAIN VERTICLE ] No input provided. Using default path: " + jsonPath
                                + Colors.RESET);
                    }
                    config.put("mode", "json");
                    config.put("json.file-path", jsonPath);
                    logger.info(Colors.GREEN + "[ MAIN VERTICLE ] JSON file path set to: " +
                            jsonPath
                            + Colors.RESET);
                    break;
                case 2:
                    logger.info(
                            Colors.MAGENTA + "[ MAIN VERTICLE ] Enter PCAP file path: " + Colors.RESET);
                    String pcapPath = scanner.nextLine();
                    if (pcapPath.trim().isEmpty()) {
                        pcapPath = config.getJsonObject("pcap").getString("file-path",
                                "src/main/resources/data/benign+slowloris_net_packets.pcap");
                        logger.info(Colors.YELLOW
                                + "[ MAIN VERTICLE ] No input provided. Using default path: " + pcapPath
                                + Colors.RESET);
                    }
                    logger.info(Colors.GREEN + "[ MAIN VERTICLE ] PCAP file path set to: " +
                            pcapPath
                            + Colors.RESET);
                    config.put("mode", "pcap");
                    config.put("pcap.file-path", pcapPath);
                    break;
                case 3:
                    logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ] Realtime ingestion method selected."
                            + Colors.RESET);
                    config.put("mode", "realtime");
                    break;
                default:
                    scanner.close();
                    throw new IllegalStateException("Unexpected value: " + choice);
            }

            scanner.close();

        }

        // Determine if ClickHouse storage is enabled
        boolean store = config.getString("store", "false").equalsIgnoreCase(
                "true");
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
        DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        vertx.deployVerticle(verticle, options).onComplete(res -> {
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
