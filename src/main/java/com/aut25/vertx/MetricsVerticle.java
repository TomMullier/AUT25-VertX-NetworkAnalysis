package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsVerticle extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(MetricsVerticle.class);

        private final ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();
        private AtomicLong received_ram = new AtomicLong(0);
        private AtomicLong received_cpu = new AtomicLong(0);

        @Override
        public void start() {

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
                                switch (type) {
                                        case String t when t.contains("FLOW_AGGREGATION_RATE"):
                                                log.info("[ METRIC ]                        {}: {} packets/s", type,
                                                                rate);
                                                break;
                                        case String t when t.contains("SYSTEM_CPU"):
                                                log.info("[ METRIC ]                        {}: {} %", type,
                                                                String.format("%.2f", rate.doubleValue()
                                                                                / received_cpu.getAndSet(0)));

                                                break;
                                        case String t when t.contains("SYSTEM_RAM"):
                                                log.info("[ METRIC ]                        {}: {} %", type,
                                                                String.format("%.2f", rate.doubleValue()
                                                                                / received_ram.getAndSet(0)));
                                                break;
                                        default:
                                                log.info("[ METRIC ]                        {}: {}", type, rate);
                                }
                        });
                        counters.clear();
                });
        }
}
