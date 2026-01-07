package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;

public class MetricsVerticle extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(MetricsVerticle.class);

        private final ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();
        private AtomicLong received_ram = new AtomicLong(0);
        private AtomicLong received_cpu = new AtomicLong(0);

        private Connection clickhouseConn;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final List<JsonObject> buffer = new ArrayList<>();
        private List<JsonObject> inflightBatch = null;

        private static final int FLUSH_INTERVAL_MS = 1000;
        private static final int MAX_BATCH_SIZE = 500;

        private final AtomicBoolean flushing = new AtomicBoolean(false);

        @Override
        public void start() throws Exception {
                log.info(Colors.GREEN + "[ METRICS VERTICLE ]              Starting MetricsVerticle..."
                                + Colors.RESET);

                // Connect to ClickHouse
                String jdbcUrl = "jdbc:clickhouse://localhost:8123/network_analysis";
                clickhouseConn = DriverManager.getConnection(jdbcUrl, "admin", "admin");

                vertx.eventBus().consumer("metrics.collect", message -> {
                        JsonObject data = (JsonObject) message.body();

                        String type = data.getString("type");
                        Long startTime = data.getLong("startTime");
                        Long endTime = data.getLong("endTime");

                        if (type == null || startTime == null || endTime == null) {
                                log.warn("[ METRIC ]                        Message invalide : {}", data.encode());
                                return;
                        }

                        long durationNs = endTime - startTime;
                        double durationMs = durationNs / 1_000_000.0;

                        log.info(
                                        "[ METRIC ]                        Type: {}, Start Time: {}, End Time: {}, Duration: {} ms",
                                        type,
                                        startTime,
                                        endTime,
                                        String.format("%.3f", durationMs));
                });

                vertx.eventBus().consumer("metrics.rates", message -> {
                        JsonObject data = (JsonObject) message.body();

                        String type = data.getString("type");
                        Long rate_per_second = data.getLong("rate_per_second");
                        String unit = data.getString("unit");
                        String verticle_id = data.getString("verticle_id", "N/A");

                        type = type + " (" + unit + ")";

                        if (type == null || rate_per_second == null || unit == null) {
                                log.warn("[ METRIC ]                        Message invalide : {}", data.encode());
                                return;
                        }

                        counters.merge(type, rate_per_second, Long::sum);
                });

                vertx.eventBus().consumer("metrics.core", message -> {
                        JsonObject data = (JsonObject) message.body();

                        String type = data.getString("type");
                        Double value = data.getDouble("value");
                        String source = data.getString("source", "N/A");

                        if (type == null || value == null) {
                                log.warn("[ METRIC ]                        Message invalide : {}", data.encode());
                                return;
                        }

                        counters.merge(type, value.longValue(), Long::sum);

                        if ("SYSTEM_CPU".equals(type)) {
                                received_cpu.addAndGet(1);
                        } else if ("SYSTEM_RAM".equals(type)) {
                                received_ram.addAndGet(1);
                        }
                });

                vertx.setPeriodic(1000, id -> {
                        if (counters.isEmpty()) {
                                log.info("[ METRIC ]                        No metrics collected in the last interval.");
                                return;
                        }

                        log.info("[ METRIC ]                        Aggregated Rates in the last 1 second:");
                        counters.forEach((type, rate) -> {
                                JsonObject batch = new JsonObject();
                                switch (type) {
                                        case String t when t.contains("FLOW_AGGREGATION_RATE"):
                                                log.info("[ METRIC ]                        {}: {} packets/s", type,
                                                                rate);
                                                batch.put("type", type).put("rate", rate).put("unit", "packets/s").put("timestamp",
                                                                System.nanoTime());
                                                break;
                                        case String t when t.contains("SYSTEM_CPU"):
                                                batch.put("type", type).put("rate", rate.doubleValue()
                                                                / received_cpu.get()).put("unit", "%").put("timestamp",
                                                                System.nanoTime());
                                                log.info("[ METRIC ]                        {}: {} %", type,
                                                                String.format("%.2f", rate.doubleValue()
                                                                                / received_cpu.getAndSet(0)));

                                                break;
                                        case String t when t.contains("SYSTEM_RAM"):
                                                batch.put("type", type).put("rate", rate.doubleValue()
                                                                / received_ram.get()).put("unit", "%").put("timestamp",
                                                                System.nanoTime());
                                                log.info("[ METRIC ]                        {}: {} %", type,
                                                                String.format("%.2f", rate.doubleValue()
                                                                                / received_ram.getAndSet(0)));
                                                break;
                                        default:
                                                log.info("[ METRIC ]                        {}: {}", type, rate);
                                }
                                buffer.add(batch);
                        });

                        counters.clear();
                });
                vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> {

                        flushMetrics();

                });
        }

        private void flushMetrics() {
                if (flushing.get() || buffer.isEmpty()) {
                        return;
                }
                flushing.set(true);

                inflightBatch = new ArrayList<>(buffer);
                buffer.clear();

                vertx.executeBlocking(promise -> {
                        try {
                                String insertSQL = "INSERT INTO metrics (type, rate, unit, timestamp) VALUES (?, ?, ?, ?)";
                                var preparedStatement = clickhouseConn.prepareStatement(insertSQL);

                                for (JsonObject metric : inflightBatch) {
                                        preparedStatement.setString(1, metric.getString("type"));
                                        preparedStatement.setDouble(2, metric.getDouble("rate"));
                                        preparedStatement.setString(3, metric.getString("unit"));
                                        preparedStatement.setLong(4, metric.getLong("timestamp"));
                                        preparedStatement.addBatch();
                                }

                                preparedStatement.executeBatch();
                                preparedStatement.close();

                                log.info("[ METRICS VERTICLE ]             Flushed {} metrics to ClickHouse",
                                                inflightBatch.size());
                                inflightBatch = null;
                                promise.complete();
                        } catch (Exception e) {
                                log.error("[ METRICS VERTICLE ]             Failed to flush metrics to ClickHouse", e);
                                // Re-add the failed batch back to the buffer for retry
                                buffer.addAll(inflightBatch);
                                inflightBatch = null;
                                promise.fail(e);
                        } finally {
                                flushing.set(false);
                        }
                }, res -> {
                        
                });
        }
}
