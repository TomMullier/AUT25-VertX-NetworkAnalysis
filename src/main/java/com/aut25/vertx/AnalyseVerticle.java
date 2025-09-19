package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import static java.lang.Thread.sleep;

public class AnalyseVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(AnalyseVerticle.class);
        KafkaConsumer<String, String> consumer;

        @Override
        public void start() {
                logger.info(Colors.GREEN + "[ ANALYSE VERTICLE ] Starting AnalyseVerticle..." + Colors.RESET);

                // Setup Kafka consumer
                Properties props = new Properties();
                props.setProperty("bootstrap.servers", "localhost:9092");
                props.setProperty("group.id", "analyse-group");
                props.setProperty("enable.auto.commit", "false");
                props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty("auto.offset.reset", "earliest");
                consumer = new KafkaConsumer<>(props);

                // Start reading from Kafka topic every 2 seconds
                readFromKafkaActionEveryDelay("network-data", 2000);
        }

        /**
         * Reads messages from a Kafka topic at regular intervals.
         * 
         * @param topic the Kafka topic to subscribe to
         * @param delay the delay in milliseconds between reads
         */
        private void readFromKafkaActionEveryDelay(String topic, long delay) {
                logger.info(Colors.CYAN + "[ ANALYSE VERTICLE ] Subscribing to Kafka topic: " + topic + Colors.RESET);

                // Subscribe to the topic
                consumer.subscribe(Arrays.asList(topic));

                // Use executeBlocking to avoid blocking the event loop
                // Read messages in a loop with the specified delay
                vertx.executeBlocking(promise -> {
                        try {
                                while (true) {
                                        ConsumerRecords<String, String> records = consumer
                                                        .poll(java.time.Duration.ofMillis(100));
                                        for (ConsumerRecord<String, String> record : records) {
                                                try{
                                                        JsonObject json = new JsonObject(record.value());
                                                logger.info(Colors.YELLOW + "[ ANALYSE VERTICLE ] Received record: "
                                                                + json.encodePrettily() + Colors.RESET);
                                                } catch (Exception e) {
                                                        logger.error(Colors.RED + "[ ANALYSE VERTICLE ] Failed to parse JSON: "
                                                                        + e.getMessage() + Colors.RESET);
                                                        logger.error(Colors.RED + "[ ANALYSE VERTICLE ] Original record: "
                                                                        + record.value() + Colors.RESET);
                                                }
                                                // TODO Process the JSON data as needed
                                                sleep(delay); // Attendre avant de lire à nouveau

                                        }
                                        if (!records.isEmpty()) {
                                                consumer.commitSync();
                                                logger.debug("[ ANALYSE VERTICLE ] Offsets committed.");
                                        }
                                }
                        } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.error(Colors.RED + "[ ANALYSE VERTICLE ] Consumer interrupted: " + e.getMessage()
                                                + Colors.RESET);
                        } catch (Exception e) {
                                logger.error("[ ANALYSE VERTICLE ] Error in consumer loop: "
                                                + e.getMessage());
                        } finally {
                                consumer.close();
                                logger.info("[ ANALYSE VERTICLE ] Kafka consumer closed.");
                        }
                }, res -> {
                        if (res.succeeded()) {
                                logger.info("[ ANALYSE VERTICLE ] Finished processing Kafka messages.");
                        } else {
                                logger.error("[ ANALYSE VERTICLE ] Failed to process Kafka messages: "
                                                + res.cause());
                        }
                });

                logger.info("[ ANALYSE VERTICLE ] Kafka consumer setup complete.");

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        consumer.close();
                        logger.info("[ ANALYSE VERTICLE ] Kafka consumer closed.");
                }));

        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ ANALYSE VERTICLE ] AnalyseVerticle stopped!" + Colors.RESET);
        }

}
