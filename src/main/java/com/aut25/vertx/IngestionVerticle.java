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

                // Get parameters from config file with default values
                String mode = config.getString("mode", "debug");

                // Creation of the event bus
                eb = vertx.eventBus();

                System.out.println("[ INGESTION VERTICLE ][ CONFIG ] Mode: " + mode);
                switch (mode) {
                        case "debug":
                                vertx.setPeriodic(5000, id -> simulateNetworkData());
                                break;
                        case "kafka":
                                // TODO : implement Kafka ingestion
                                System.out.println("[ INGESTION VERTICLE ] Kafka ingestion not implemented yet.");
                                break;
                        case "realtime":
                                // TODO : implement real-time ingestion
                                vertx.setPeriodic(1000, id -> ingestInRealTime());
                                System.out.println("[ INGESTION VERTICLE ] Real-time ingestion not implemented yet.");
                                break;
                        case "from-file":
                                JsonObject fileConfig = config.getJsonObject("from-file", new JsonObject());
                                String filePath = fileConfig.getString("file-path", "data/sample_data.txt");
                                int interval = fileConfig.getInteger("ingestion-interval-ms", 1000);
                                System.out.println("[ INGESTION VERTICLE ][ CONFIG ] File path: " + filePath);
                                System.out.println("[ INGESTION VERTICLE ][ CONFIG ] Ingestion interval (ms): "
                                                + interval);

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
                                AtomicReference<Iterator<JsonObject>> iteratorRef = new AtomicReference<>(
                                                records.iterator());

                                vertx.setPeriodic(interval, id -> {
                                        Iterator<JsonObject> it = iteratorRef.get();
                                        if (!it.hasNext()) {
                                                it = records.iterator(); // recrée iterator
                                                iteratorRef.set(it); // met à jour la référence
                                                System.out.println(
                                                                "[ INGESTION VERTICLE ] End of file reached. Looping again...");
                                        }
                                        JsonObject record = it.next();
                                        eb.publish("network.data", record);
                                        System.out.println("[ INGESTION VERTICLE ] Published record from file: \n"
                                                        + record.encodePrettily());
                                });

                                break;
                        default:
                                System.out.println(
                                                "[ INGESTION VERTICLE ] Unknown mode. No ingestion will be performed.");
                                break;
                }

                System.out.println("[ INGESTION VERTICLE ] IngestionVerticle started!");

        }

        /* ------------------------------- Mode DEBUG ------------------------------- */
        private void simulateNetworkData() {
                JsonObject networkData = new JsonObject()
                                .put("srcIp", "10.0.0.1")
                                .put("dstIp", "10.0.0.2")
                                .put("protocol", "TCP")
                                .put("bytes", 1000);
                eb.publish("network.data", networkData);
                System.out.println("[ INGESTION VERTICLE ] Published simulated network data: \n"
                                + networkData.encodePrettily());
        }

        /* ------------------------------- Mode Kafka ------------------------------- */
        // TODO : implement Kafka ingestion
        private void ingestFromKafka(JsonObject kafkaConfig) {
        }

        private JsonObject captureRealNetworkData() {
                // Ici tu mettrais la logique de capture réseau réelle
                return new JsonObject()
                                .put("srcIp", "10.0.0.3")
                                .put("dstIp", "10.0.0.4")
                                .put("protocol", "UDP")
                                .put("bytes", 2000);
        }

        /* ----------------------------- Mode Realtime ------------------------------ */
        // TODO : implement real-time ingestion
        private void ingestInRealTime() {
                // Placeholder for real-time ingestion logic
        }

        /* ----------------------------- Mode From-File ----------------------------- */

        /* -------------------------------------------------------------------------- */
        @Override
        public void stop() {
                System.out.println("[ INGESTION VERTICLE ] IngestionVerticle stopped!");
        }
}