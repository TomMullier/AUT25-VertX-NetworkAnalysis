package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

public class SystemMetricsVerticle extends AbstractVerticle {

        private OperatingSystemMXBean osBean;

        @Override
        public void start() {
                osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

                // Publier CPU & RAM toutes les secondes
                vertx.setPeriodic(50, id -> {
                        double cpu = osBean.getProcessCpuLoad() * 100; // %
                        long ramUsed = osBean.getTotalPhysicalMemorySize() - osBean.getFreePhysicalMemorySize();
                        long ramTotal = osBean.getTotalPhysicalMemorySize();
                        double ramPercent = ((double) ramUsed / ramTotal) * 100;

                        vertx.eventBus().send("metrics.core", new JsonObject()
                                        .put("type", "SYSTEM_CPU")
                                        .put("source", "SystemMetrics")
                                        .put("value", cpu));

                        vertx.eventBus().send("metrics.core", new JsonObject()
                                        .put("type", "SYSTEM_RAM")
                                        .put("source", "SystemMetrics")
                                        .put("value", ramPercent));
                });
        }
}
