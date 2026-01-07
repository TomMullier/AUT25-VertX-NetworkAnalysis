package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsVerticle extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(MetricsVerticle.class);

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
        }
}
