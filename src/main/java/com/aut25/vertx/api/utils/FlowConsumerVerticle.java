package com.aut25.vertx.api.utils;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.api.WebServerVerticle;
import com.aut25.vertx.utils.Colors;
import io.vertx.core.json.JsonObject;

public class FlowConsumerVerticle extends AbstractVerticle {

    private final List<io.vertx.core.http.ServerWebSocket> clients = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(FlowConsumerVerticle.class);

    public void registerClient(io.vertx.core.http.ServerWebSocket ws) {
        clients.add(ws);
        ws.closeHandler(v -> clients.remove(ws));
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", "vertx-consumer");
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", "true");

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

        consumer.handler(record -> {
            try {
                String value = record.value();
                if (value == null || value.isEmpty() || value.equals("reset")) {
                    // Display flow info
                    logger.debug("[ FLOW CONSUMER VERTICLE ]        Received reset flow: {}",
                            record);

                    return;
                }
                JsonObject flow = new JsonObject(value);
                vertx.eventBus().publish("flows.data", flow);
            } catch (Exception e) {
                logger.error("[ FLOW CONSUMER VERTICLE ]        Error processing flow: {}", e.getMessage());
            }
        });

        consumer.subscribe("network-flows")
                .onSuccess(ok -> {
                    logger.info(Colors.CYAN + "[ FLOW CONSUMER VERTICLE ]        Subscribed to topic 'network-flows'"
                            + Colors.RESET);
                    startPromise.complete();
                })
                // On failure fail and log error
                .onFailure(failure -> {
                    logger.error(Colors.RED
                            + "[ FLOW CONSUMER VERTICLE ]        Failed to subscribe to topic 'network-flows': "
                            + failure.getMessage() + Colors.RESET);
                    startPromise.fail(failure);
                });

    }
}
