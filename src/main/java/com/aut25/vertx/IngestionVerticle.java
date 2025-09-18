package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Iterator;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class IngestionVerticle extends AbstractVerticle {

        public EventBus eb;

        @Override
        public void start() throws Exception {
                // Config file : get debug mode
                JsonObject config = new JsonObject(
                                new String(Files.readAllBytes(Paths.get("src/main/resources/config.json"))));

                /*
                 * Get mode from config file.
                 * Can be :
                 * - json (default for debug)
                 * - pcap
                 * - realtime
                 */
                String mode = config.getString("mode", "json");

                // Creation of the event bus
                eb = vertx.eventBus();

                System.out.println("[ INGESTION VERTICLE ][ CONFIG ] Mode: " + mode);
                switch (mode) {
                        case "json":
                                /* ------------------------ Parameters for json mode ------------------------ */
                                JsonObject fileConfig = config.getJsonObject("json", new JsonObject());
                                // Get path and ingestion interval from config file
                                String filePath = fileConfig.getString("file-path", "data/sample_data.json");
                                int interval = fileConfig.getInteger("ingestion-interval-ms", 1000);
                                System.out.println("[ INGESTION VERTICLE ][ CONFIG ] File path: " + filePath);
                                System.out.println("[ INGESTION VERTICLE ][ CONFIG ] Ingestion interval (ms): "
                                                + interval);

                                /* ------------------------ READ FILE AND PARSE JSON ------------------------ */
                                List<JsonObject> records;
                                try {
                                        String content = Files.readString(Paths.get(filePath), StandardCharsets.UTF_8);
                                        ObjectMapper mapper = new ObjectMapper();

                                        List<Map<String, Object>> listOfMaps = mapper.readValue(
                                                        content, new TypeReference<List<Map<String, Object>>>() {
                                                        });

                                        records = listOfMaps.stream()
                                                        .map(JsonObject::mapFrom)
                                                        .toList();

                                } catch (Exception e) {
                                        System.err.println("[ INGESTION VERTICLE ] Failed to read or parse file: "
                                                        + e.getMessage());
                                        return;
                                }

                                /* --------------- Publication of records at regular intervals -------------- */
                                // AtomicReference to keep track of the iterator state
                                AtomicReference<Iterator<JsonObject>> iteratorRef = new AtomicReference<>(
                                                records.iterator());

                                vertx.setPeriodic(interval, id -> {
                                        Iterator<JsonObject> it = iteratorRef.get();
                                        if (!it.hasNext()) {
                                                // Reset iterator if end of list is reached to loop
                                                it = records.iterator();
                                                iteratorRef.set(it);
                                                System.out.println(
                                                                "[ INGESTION VERTICLE ] End of file reached. Looping again...");
                                        }
                                        // Publication of the next record
                                        JsonObject record = it.next();
                                        eb.publish("network.data", record);
                                        System.out.println("[ INGESTION VERTICLE ] Published record from file: \n"
                                                        + record.encodePrettily());
                                });

                                break;

                        case "pcap":
                                // TODO : implement Kafka ingestion
                                System.out.println("[ INGESTION VERTICLE ] Kafka ingestion not implemented yet.");
                                break;
                                
                        case "realtime":
                                // TODO : implement real-time ingestion
                                vertx.setPeriodic(1000, id -> ingestInRealTime());
                                System.out.println("[ INGESTION VERTICLE ] Real-time ingestion not implemented yet.");
                                break;
                        default:
                                System.out.println(
                                                "[ INGESTION VERTICLE ] Unknown mode. No ingestion will be performed.");
                                break;
                }

                System.out.println("[ INGESTION VERTICLE ] IngestionVerticle started!");

        }

        /* ------------------------------- Mode Kafka ------------------------------- */
        // TODO : implement Kafka ingestion
        private void ingestFromKafka(JsonObject kafkaConfig) {
        }

        /* ----------------------------- Mode Realtime ------------------------------ */
        // TODO : implement real-time ingestion
        private void ingestInRealTime() {
                // Placeholder for real-time ingestion logic
        }

        /* -------------------------------------------------------------------------- */
        @Override
        public void stop() {
                System.out.println("[ INGESTION VERTICLE ] IngestionVerticle stopped!");
        }
}