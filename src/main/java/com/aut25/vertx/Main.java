package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        System.out.println("[ MAIN VERTICLE ] Starting MainVerticle...");

        /* ---------------------- Load configuration from file ---------------------- */
        JsonObject config = new JsonObject(
                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));
        System.out.println("[ MAIN VERTICLE ] Loaded configuration: " + config.encodePrettily());

        /* ---------------------- Deploy the IngestionVerticle ---------------------- */
        vertx.deployVerticle(new IngestionVerticle(), res -> {
            if (res.succeeded()) {
                System.out.println("[ MAIN VERTICLE ] IngestionVerticle deployed successfully!");
            } else {
                System.err.println("[ MAIN VERTICLE ] Failed to deploy IngestionVerticle: " + res.cause());
            }
        });

        /* ---------------------- Deploy the AnalyseVerticle ---------------------- */
        vertx.deployVerticle(new AnalyseVerticle(), res -> {
            if (res.succeeded()) {
                System.out.println("[ MAIN VERTICLE ] AnalyseVerticle deployed successfully!");
            } else {
                System.err.println("[ MAIN VERTICLE ] Failed to deploy AnalyseVerticle: " + res.cause());
            }
        });

        /* ------------------------ Deploy the ApiVerticle ------------------------- */
        // vertx.deployVerticle(new ApiVerticle(), res -> {
        // if (res.succeeded()) {
        // System.out.println("[ MAIN VERTICLE ] ApiVerticle deployed successfully!");
        // } else {
        // System.err.println("[ MAIN VERTICLE ] Failed to deploy ApiVerticle: " +
        // res.cause());
        // }
        // });

        startPromise.complete(); // Signale que le MainVerticle est prêt
    }

    @Override
    public void stop() {
        System.out.println("[ MAIN VERTICLE ] MainVerticle stopped!");

    }
}
