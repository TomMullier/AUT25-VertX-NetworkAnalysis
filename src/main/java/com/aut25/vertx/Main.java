package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;

import static java.lang.Thread.sleep;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;
import com.aut25.vertx.utils.Flow;
import com.aut25.vertx.utils.NDPIWrapper;
import com.aut25.vertx.utils.NdpiFlowWrapper;
import com.aut25.vertx.api.*;
import com.aut25.vertx.api.utils.*;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.BufferedWriter;

public class Main extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // List to track deployed verticle IDs

    public String ingestionMethod;
    public final List<String> deploymentIds = new ArrayList<>();
    public List<AbstractVerticle> verticles = new ArrayList<>();
    private JsonObject config;
    private SharedData sharedData;
    private Scanner scanner;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 Starting MainVerticle..." + Colors.RESET);
        // Load configuration from JSON file
        config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        sharedData = loadSharedData(vertx, config);

        logger.debug("[ MAIN VERTICLE ] Loaded configuration: " + sharedData.getLocalMap("config").toString());

        // Check the mode in the configuration
        String mode = config.getString("mode", "menu");
        boolean exitMenu = false;
        boolean exitProgram = false;
        scanner = new Scanner(System.in);
        if ("menu".equalsIgnoreCase(mode)) {
            logger.info(
                    Colors.YELLOW + "[ MAIN VERTICLE ] [ CONFIG ]      Mode set to 'menu'. Launching interactive menu."
                            + Colors.RESET);
            exitMenu = false;
        } else {
            logger.info(Colors.YELLOW + "[ MAIN VERTICLE ] [ CONFIG ]      Mode set to '" + mode
                    + "'. Skipping interactive menu."
                    + Colors.RESET);
            exitMenu = true;
        }

        while (!exitMenu) {
            // --- Menu principal ---
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 Choose ingestion method:");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 1. PCAP Instant (provide file path)");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 2. PCAP Replay (provide file path)");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 3. Realtime");
            logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 4. Exit" + Colors.RESET);

            int choice = -1;
            while (choice < 1 || choice > 4) {
                logger.info(
                        Colors.MAGENTA + "[ MAIN VERTICLE ]                 Enter your choice (1-4): " + Colors.RESET);
                try {
                    choice = Integer.parseInt(scanner.nextLine());
                } catch (NumberFormatException e) {
                    logger.error("                 Invalid input. Please enter a number between 1 and 4.");
                }
            }

            switch (choice) {
                case 1: // PCAP Instant

                    ingestionMethod = "pcap-instant";
                    logger.info(
                            Colors.MAGENTA + "[ MAIN VERTICLE ]                 PCAP Instant ingestion method selected."
                                    + Colors.RESET);
                    logger.info(Colors.MAGENTA
                            + "[ MAIN VERTICLE ]                 Enter PCAP file path (or type 'menu' to return): "
                            + Colors.RESET);
                    String pcapPath_instant = scanner.nextLine();
                    if ("menu".equalsIgnoreCase(pcapPath_instant.trim())) {
                        break; // revient au menu principal
                    }
                    if (pcapPath_instant.trim().isEmpty()) {
                        pcapPath_instant = config.getJsonObject("pcap").getString("file-path",
                                "_tests/benchmark/000_PCAP/reference.pcap");
                        logger.info(Colors.YELLOW
                                + "[ MAIN VERTICLE ]                 No input provided. Using default path: "
                                + pcapPath_instant + Colors.RESET);
                    }
                    config.put("mode", "pcap-instant");
                    config.put("pcap.file-path", pcapPath_instant);

                    JsonObject pcapConfig_instant = new JsonObject()
                            .put("file-path", pcapPath_instant)
                            .put("delay", "false");
                    sharedData.getLocalMap("config").put("pcap", pcapConfig_instant);
                    sharedData.getLocalMap("config").put("ingestionMethod", ingestionMethod);
                    sharedData.getLocalMap("config").put("mode", "pcap-instant");

                    logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 PCAP file path set to: "
                            + pcapPath_instant
                            + Colors.RESET);
                    exitMenu = true;
                    break;

                case 2: // PCAP
                    ingestionMethod = "pcap";
                    logger.info(Colors.MAGENTA + "[ MAIN VERTICLE ]                 PCAP ingestion method selected."
                            + Colors.RESET);
                    logger.info(Colors.MAGENTA
                            + "[ MAIN VERTICLE ]                 Enter PCAP file path (or type 'menu' to return): "
                            + Colors.RESET);
                    String pcapPath = scanner.nextLine();
                    if ("menu".equalsIgnoreCase(pcapPath.trim())) {
                        break; // revient au menu principal
                    }
                    if (pcapPath.trim().isEmpty()) {
                        pcapPath = config.getJsonObject("pcap").getString("file-path",
                                "_tests/benchmark/000_PCAP/reference.pcap");
                        logger.info(Colors.YELLOW
                                + "[ MAIN VERTICLE ]                 No input provided. Using default path: "
                                + pcapPath + Colors.RESET);
                    } else {
                        if (!Files.exists(Paths.get(pcapPath))) {
                            logger.error(Colors.RED + "[ MAIN VERTICLE ]                 PCAP file not found at: "
                                    + pcapPath + Colors.RESET);
                            break; // revient au menu principal
                        }
                    }
                    config.put("mode", "pcap");
                    config.put("pcap.file-path", pcapPath);

                    JsonObject pcapConfig = new JsonObject()
                            .put("file-path", pcapPath)
                            .put("delay", "true");
                    sharedData.getLocalMap("config").put("pcap", pcapConfig);
                    sharedData.getLocalMap("config").put("ingestionMethod", ingestionMethod);
                    sharedData.getLocalMap("config").put("mode", "pcap");

                    logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 PCAP file path set to: " + pcapPath
                            + Colors.RESET);
                    exitMenu = true;
                    break;

                case 3: // Realtime
                    ingestionMethod = "realtime";
                    logger.info(
                            Colors.MAGENTA + "[ MAIN VERTICLE ]                 Realtime ingestion method selected."
                                    + Colors.RESET);
                    config.put("mode", "realtime");

                    JsonObject realtimeConfig = new JsonObject();
                    sharedData.getLocalMap("config").put("realtime", realtimeConfig);
                    sharedData.getLocalMap("config").put("ingestionMethod", ingestionMethod);
                    sharedData.getLocalMap("config").put("mode", "realtime");
                    exitMenu = true;
                    break;

                case 4: // Exit
                    exitMenu = true;
                    exitProgram = true;
                    logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 Exiting menu." + Colors.RESET);
                    vertx.close(ar -> {
                        if (ar.succeeded()) {

                            logger.info(Colors.GREEN
                                    + "[ MAIN VERTICLE ]                 Vert.x closed successfully. Goodbye!"
                                    + Colors.RESET);
                        } else {
                            logger.error(
                                    Colors.RED + "[ MAIN VERTICLE ]                 Error closing Vert.x: " + ar.cause()
                                            + Colors.RESET);
                        }
                    });

                    break;

                default:
                    throw new IllegalStateException("Unexpected value: " + choice);
            }
        }

        if (exitProgram) {
            logger.info(Colors.YELLOW + "[ MAIN VERTICLE ]                 Exiting before deployment as requested."
                    + Colors.RESET);
            startPromise.complete();
            return; // empêche la suite du start()
        }

        // log shared data config
        logger.debug("[ MAIN VERTICLE ]                 Shared Data Config: "
                + sharedData.getLocalMap("config").toString());

        // Determine if ClickHouse storage is enabled
        boolean store = config.getString("store", "false").equalsIgnoreCase(
                "true");
        logger.debug(Colors.YELLOW + "[ MAIN VERTICLE ] [ CONFIG ]      Store configuration: " + store + Colors.RESET);

        // Créer la liste de verticles à déployer
        long start_deployement = System.nanoTime();
        verticles.add(new MetricsVerticle());
        // verticles.add(new BenchmarkVerticle());
        // verticles.add(new FlowConsumerVerticle());
        WebServerVerticle webServerVerticle = new WebServerVerticle(this);
        // :verticles.add(webServerVerticle);
        verticles.add(new IngestionVerticle());
        verticles.add(new PcapCoordinatorVerticle());

        if (store) {
            verticles.add(new ClickHousePacketVerticle());
            verticles.add(new ClickHouseFlowsVerticle());
        } else {
            logger.info(Colors.YELLOW
                    + "[ MAIN VERTICLE ]                 Skipping ClickHouse verticles as per configuration."
                    + Colors.RESET);
        }
        sharedData.getLocalMap("config").put("ndpi_initialized", false);
        int numAggregators = 50;
        for (int i = 0; i < numAggregators; i++) {
            FlowAggregatorVerticle verticle = new FlowAggregatorVerticle();
            DeploymentOptions options = new DeploymentOptions();
            options.setConfig(config);
            options.setInstances(1);
            options.setWorker(true);
            vertx.deployVerticle(verticle, options);
        }

        AtomicInteger readyCount = new AtomicInteger(0);
        vertx.eventBus().consumer("flow.aggregator.ready", msg -> {
            int ready = readyCount.incrementAndGet();
            logger.debug(Colors.GREEN + "[ MAIN VERTICLE ]                 FlowAggregatorVerticle ready count: " + ready
                    + Colors.RESET);
            if (ready == numAggregators) {
                logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 " + numAggregators
                        + " FlowAggregatorVerticles are ready."
                        + Colors.RESET);
                // Déploiement séquentiel
                deployVerticlesSequentially(verticles, startPromise);

                long end_deployment = System.nanoTime();
                vertx.eventBus().send(
                        "metrics.collect",
                        new JsonObject()
                                .put("type", "VERTICLE_DEPLOYMENT")
                                .put("startTime", start_deployement)
                                .put("endTime", end_deployment));
            }
        });
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
        scanner.close();
        // Stopper tous les verticles explicitement
        if (deploymentIds.isEmpty()) {
            logger.info(Colors.RED + "[ MAIN VERTICLE ]                 No verticles to undeploy." + Colors.RESET);
            stopPromise.complete();
            return;
        }
    }

    public void redeployIngestionVerticle() {
        logger.info(
                Colors.GREEN + "[ MAIN VERTICLE ]                 Redeploying ingestion verticle with new settings..."
                        + Colors.RESET);
        // update config
        LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");
        JsonObject c = new JsonObject(map);

        String mode = c.getString("ingestionMethod");
        String filePath = c.getJsonObject(mode).getString("file-path");

        logger.info(Colors.YELLOW + "[ MAIN VERTICLE ]                 New settings for ingestion: mode=" + mode
                + ", file-path=" + filePath + Colors.RESET);

        config.put("mode", mode);
        sharedData.getLocalMap("config").put("mode", mode);
        if (mode.equals("pcap")) {
            config.put("pcap.file-path", filePath);
            sharedData.getLocalMap("config").put("pcap", new JsonObject().put("file-path", filePath));
        } else if (mode.equals("json")) {
            config.put("json.file-path", filePath);
            sharedData.getLocalMap("config").put("json",
                    new JsonObject()
                            .put("file-path", filePath)
                            .put("ingestion-interval-ms",
                                    config.getJsonObject("json").getInteger("ingestion-interval-ms", 1000)));
        }
        // Update options
        DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);

        // Find and undeploy existing IngestionVerticle
        for (String id : deploymentIds) {
            vertx.deploymentIDs().forEach(deploymentId -> {
                vertx.getOrCreateContext().get("mainVerticle");
            });
        }

        vertx.deployVerticle(new IngestionVerticle(), options).onComplete(res -> {
            if (res.succeeded()) {
                logger.info(Colors.GREEN
                        + "[ MAIN VERTICLE ]                 IngestionVerticle redeployed successfully!"
                        + Colors.RESET);
            } else {
                logger.error(
                        Colors.RED + "[ MAIN VERTICLE ]                 Failed to redeploy IngestionVerticle: "
                                + res.cause() + Colors.RESET);
            }
        });
    }

    public void redeployFlowAggregatorVerticle() {
        logger.info(Colors.GREEN + "[ MAIN VERTICLE ]                 Redeploying FlowAggregator verticle..."
                + Colors.RESET);
        // update config
        LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");
        JsonObject c = new JsonObject(map);

        String mode = c.getString("ingestionMethod");
        logger.info(Colors.YELLOW + "[ MAIN VERTICLE ]                 New settings for FlowAggregator: mode=" + mode
                + Colors.RESET);

        sharedData.getLocalMap("config").put("mode", mode);
        config.put("mode", mode);
        // Update options
        DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);

        // Find and undeploy existing FlowAggregatorVerticle
        for (String id : deploymentIds) {
            vertx.deploymentIDs().forEach(deploymentId -> {
                vertx.getOrCreateContext().get("mainVerticle");
            });
        }

        vertx.deployVerticle(new FlowAggregatorVerticle(), options).onComplete(res -> {
            if (res.succeeded()) {
                logger.info(Colors.GREEN
                        + "[ MAIN VERTICLE ]                 FlowAggregatorVerticle redeployed successfully!"
                        + Colors.RESET);
            } else {
                logger.error(
                        Colors.RED + "[ MAIN VERTICLE ]                 Failed to redeploy FlowAggregatorVerticle: "
                                + res.cause() + Colors.RESET);
            }
        });
    }

    private SharedData loadSharedData(io.vertx.core.Vertx vertx, JsonObject config) {
        SharedData sharedData = vertx.sharedData();
        sharedData.getLocalMap("config").put("ingestionMethod", config.getString("ingestionMethod", "none"));
        // Put all elements in shared data config
        sharedData.getLocalMap("config").put("http.port", config.getInteger("http.port", 8080));
        sharedData.getLocalMap("config").put("store", config.getString("store", "true"));
        sharedData.getLocalMap("config").put("mode", config.getString("mode", "menu"));

        JsonObject jsonConfig = config.getJsonObject("json", new JsonObject()
                .put("file-path", "src/main/resources/data/network-data.json")
                .put("ingestion-interval-ms", 1000));
        sharedData.getLocalMap("config").put("json", jsonConfig);

        JsonObject pcapConfig = config.getJsonObject("pcap", new JsonObject()
                .put("file-path", "_tests/benchmark/000_PCAP/reference.pcap")
                .put("delay", "true"));
        sharedData.getLocalMap("config").put("pcap", pcapConfig);

        JsonObject realtimeTestConfig = config.getJsonObject("realtime_test", new JsonObject()
                .put("interface", "ens160"));
        sharedData.getLocalMap("config").put("realtime_test", realtimeTestConfig);

        JsonObject realtimeConfig = config.getJsonObject("realtime", new JsonObject());
        sharedData.getLocalMap("config").put("realtime", realtimeConfig);

        logger.info("[ MAIN VERTICLE ]                 Based configuration : "
                + sharedData.getLocalMap("config").toString());

        return sharedData;
    }
}
