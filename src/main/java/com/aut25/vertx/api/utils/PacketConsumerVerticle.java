package com.aut25.vertx.api.utils;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.api.WebServerVerticle;
import com.aut25.vertx.utils.Colors;
import io.vertx.core.json.JsonObject;

public class PacketConsumerVerticle extends AbstractVerticle {

    private final List<io.vertx.core.http.ServerWebSocket> clients = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(PacketConsumerVerticle.class);

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
                    logger.debug("[ PACKET CONSUMER VERTICLE ]      Received reset packet: {}", record);
                    return;
                }

                JsonObject packet = new JsonObject(value);
                vertx.eventBus().publish("packets.data", packet);
            } catch (Exception e) {
                logger.error("[ PACKET CONSUMER VERTICLE ]      Error processing packet: {}", e.getMessage());
            }
        });

        // consumer.subscribe("network-data")
        // .onSuccess(ok -> {
        // logger.info(Colors.CYAN + "[ PACKET CONSUMER VERTICLE ] Subscribed to topic
        // 'network-data'"
        // + Colors.RESET);
        // startPromise.complete();
        // })
        // .onFailure(failure -> {
        // logger.error(Colors.RED
        // + "[ PACKET CONSUMER VERTICLE ] Failed to subscribe to topic 'network-data':
        // "
        // + failure.getMessage() + Colors.RESET);
        // startPromise.fail(failure);
        // });

    }
}
