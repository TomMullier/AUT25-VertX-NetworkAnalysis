package com.aut25.vertx.api.utils;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.api.WebServerVerticle;
import com.aut25.vertx.utils.Colors;

public class FlowConsumerVerticle extends AbstractVerticle {

    private final List<io.vertx.core.http.ServerWebSocket> clients = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);

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
            vertx.eventBus().publish("flows.data", record.value());
        });

        consumer.subscribe("network-flows")
                .onSuccess(ok -> {
                    logger.info(Colors.CYAN + "[ FLOW CONSUMER VERTICLE ]        Subscribed to topic 'network-flows'"
                            + Colors.RESET);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);

    }
}
