package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;

public class IngestionVerticle extends AbstractVerticle {

    @Override
    public void start() {
        System.out.println("[ INGESTION VERTICLE ] IngestionVerticle started!");
        // Ici, tu peux démarrer Kafka consumer, traitement de flux, etc.
    }
}