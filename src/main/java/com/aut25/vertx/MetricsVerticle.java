package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.utils.Colors;

/**
 * Verticle de collecte/agrégation des métriques :
 *  - Consomme des évènements EventBus ("metrics.collect", "metrics.rates", "metrics.core", "ingestion.stats")
 *  - Agrège par période de 1s et log les métriques
 *  - Envoie les métriques en batch dans ClickHouse (JDBC)
 *  - Affiche un HUD en haut à droite (console ANSI) avec les valeurs clés
 */
public class MetricsVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(MetricsVerticle.class);

    /** Agrégats par type (ex: "FLOW_AGGREGATION_RATE", "SYSTEM_CPU", "SYSTEM_RAM") */
    private final ConcurrentHashMap<String, Long> counters = new ConcurrentHashMap<>();
    /** Compteurs d’échantillons pour faire une moyenne CPU/RAM sur la seconde */
    private final AtomicLong receivedRamSamples = new AtomicLong(0);
    private final AtomicLong receivedCpuSamples = new AtomicLong(0);

    /** Connexion ClickHouse */
    private Connection clickhouseConn;

    /** Buffer des métriques à flusher + contrôle de flush */
    private final List<JsonObject> buffer = new ArrayList<>();
    private List<JsonObject> inflightBatch = null;
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    /** Périodicité du flush (ms) et batch max (= sécurité; ClickHouse gère bien du gros batch) */
    private static final int FLUSH_INTERVAL_MS = 1_000;
    private static final int MAX_BATCH_SIZE = 5_000;

    /** HUD : dernières valeurs affichées */
    private ConsoleHud hud;
    private volatile String lastSystemRam = "N/A";
    private volatile String lastSystemCpu = "N/A";
    private volatile String lastFlowAggRate = "N/A";
    private final AtomicLong lastIngestReceived = new AtomicLong(0);
    private final AtomicLong lastIngestQueue = new AtomicLong(0);
    private final AtomicLong lastFlushCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, String> latestHudMetrics = new ConcurrentHashMap<>();

    @Override
    public void start() throws Exception {
        log.info(Colors.GREEN + "[ METRICS VERTICLE ]             Starting MetricsVerticle..." + Colors.RESET);

        // Connexion ClickHouse
        // NOTE: adapte l’URL, l’utilisateur et le mot de passe si nécessaire
        final String jdbcUrl = "jdbc:clickhouse://localhost:8123/network_analysis";
        clickhouseConn = DriverManager.getConnection(jdbcUrl, "admin", "admin");

        // --- Consumers EventBus ---

        // Mesures ponctuelles avec start/endTime -> on log la durée
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
            log.debug("[ METRIC ]                        Type: {}, Start: {}, End: {}, Duration: {} ms",
                    type, startTime, endTime, String.format("%.3f", durationMs));
        });

        // Débits par seconde (ex: "FLOW_AGGREGATION_RATE", unit="packets/s")
        vertx.eventBus().consumer("metrics.rates", message -> {
            JsonObject data = (JsonObject) message.body();
            String type = data.getString("type");
            Long ratePerSecond = data.getLong("rate_per_second");
            String unit = data.getString("unit");

            if (type == null || ratePerSecond == null || unit == null) {
                log.warn("[ METRIC ]                        Message invalide : {}", data.encode());
                return;
            }

            // Agrégation brute (1 seconde)
            counters.merge(type, ratePerSecond, Long::sum);
        });

        // Métriques cœur (CPU/RAM) en continu -> on somme et on comptera le nombre d’échantillons pour moyenne
        vertx.eventBus().consumer("metrics.core", message -> {
            JsonObject data = (JsonObject) message.body();
            String type = data.getString("type");
            Double value = data.getDouble("value");

            if (type == null || value == null) {
                log.warn("[ METRIC ]                        Message invalide : {}", data.encode());
                return;
            }

            counters.merge(type, value.longValue(), Long::sum);

            if ("SYSTEM_CPU".equals(type)) {
                receivedCpuSamples.incrementAndGet();
            } else if ("SYSTEM_RAM".equals(type)) {
                receivedRamSamples.incrementAndGet();
            }
        });

        // (Optionnel) Statistiques d’ingestion pour HUD
        // Émettre un JsonObject: {"received": long, "queue": long}
        vertx.eventBus().consumer("ingestion.stats", message -> {
            JsonObject data = (JsonObject) message.body();
            Long received = data.getLong("received");
            Long queue = data.getLong("queue");
            if (received != null) {
                lastIngestReceived.set(received);
                latestHudMetrics.put("INGEST.received", String.valueOf(received));
            }
            if (queue != null) {
                lastIngestQueue.set(queue);
                latestHudMetrics.put("INGEST.queue", String.valueOf(queue));
            }
        });

        // --- HUD : seulement en TTY interactif (désactivable via INTERACTIVE_HUD=false) ---
        boolean interactive = System.console() != null;
        String envHud = System.getenv("INTERACTIVE_HUD");
        if ("false".equalsIgnoreCase(envHud)) {
            interactive = false;
        }
        if (interactive) {
            // largeur 48 colonnes, hauteur 12 lignes (ajuste selon tes envies)
            hud = new ConsoleHud(48, 12);

            // Rendu HUD chaque seconde
            vertx.setPeriodic(1_000, t -> {
                Map<String, String> metrics = buildHudMetrics();
                hud.render(metrics);
            });
        }

        // --- Agrégat visuel + préparation buffer ClickHouse (chaque seconde) ---
        vertx.setPeriodic(1_000, id -> {
            if (counters.isEmpty()) {
                log.debug("[ METRIC ]                        No metrics collected in the last interval.");
                return;
            }

            log.debug("[ METRIC ]                        Aggregated Rates in the last 1 second:");

            // Snapshot local pour itérer proprement
            Map<String, Long> snapshot = new LinkedHashMap<>(counters);
            counters.clear();

            long nowNs = System.nanoTime();

            for (Map.Entry<String, Long> e : snapshot.entrySet()) {
                String type = e.getKey();
                long sum = e.getValue();

                JsonObject entry = new JsonObject().put("type", type).put("timestamp", nowNs);

                if ("FLOW_AGGREGATION_RATE".equals(type)) {
                    // Débit agrégé (packets/s)
                    log.debug("[ METRIC ]                        {}: {} packets/s", type, sum);
                    entry.put("rate", sum).put("unit", "packets/s");
                    lastFlowAggRate = String.valueOf(sum);
                    latestHudMetrics.put("FLOW_AGG_RATE", sum + " packets/s");
                    buffer.add(entry);
                    continue;
                }

                if ("SYSTEM_CPU".equals(type)) {
                    long samples = receivedCpuSamples.getAndSet(0);
                    double avg = samples > 0 ? (sum * 1.0) / samples : 0.0;
                    log.debug("[ METRIC ]                        {}: {} %", type, String.format("%.2f", avg));
                    entry.put("rate", avg).put("unit", "%");
                    lastSystemCpu = String.format("%.2f", avg);
                    latestHudMetrics.put("SYSTEM_CPU", lastSystemCpu + " %");
                    buffer.add(entry);
                    continue;
                }

                if ("SYSTEM_RAM".equals(type)) {
                    long samples = receivedRamSamples.getAndSet(0);
                    double avg = samples > 0 ? (sum * 1.0) / samples : 0.0;
                    log.debug("[ METRIC ]                        {}: {} %", type, String.format("%.2f", avg));
                    entry.put("rate", avg).put("unit", "%");
                    lastSystemRam = String.format("%.2f", avg);
                    latestHudMetrics.put("SYSTEM_RAM", lastSystemRam + " %");
                    buffer.add(entry);
                    continue;
                }

                // Autres types : on log et on stocke tel quel, sans unité spécifique
                log.debug("[ METRIC ]                        {}: {}", type, sum);
                entry.put("rate", sum).put("unit", "");
                latestHudMetrics.put(type, String.valueOf(sum));
                buffer.add(entry);
            }
        });

        // --- Flush ClickHouse périodique ---
        vertx.setPeriodic(FLUSH_INTERVAL_MS, id -> flushMetrics());
    }

    @Override
    public void stop() throws Exception {
        if (hud != null) {
            hud.restore();
        }
        try {
            if (clickhouseConn != null && !clickhouseConn.isClosed()) {
                clickhouseConn.close();
            }
        } catch (Exception ignore) { }
    }

    private void flushMetrics() {
        if (flushing.get()) return;

        if (buffer.isEmpty()) return;

        flushing.set(true);
        try {
            // Copie le buffer et vide la source
            inflightBatch = new ArrayList<>(buffer);
            buffer.clear();

            // Découpe en sous-batchs si besoin
            int size = inflightBatch.size();
            int from = 0;

            while (from < size) {
                int to = Math.min(from + MAX_BATCH_SIZE, size);
                List<JsonObject> sub = inflightBatch.subList(from, to);
                writeBatch(sub);
                from = to;
            }

            lastFlushCount.addAndGet(size);
            latestHudMetrics.put("CLICKHOUSE.flushes", String.valueOf(lastFlushCount.get()));
            log.debug("[ METRICS VERTICLE ]             Flushed {} metrics to ClickHouse", size);
            inflightBatch = null;
        } catch (Exception e) {
            log.error("[ METRICS VERTICLE ]             Failed to flush metrics to ClickHouse", e);
            // Réinjecte le batch échoué
            if (inflightBatch != null) buffer.addAll(inflightBatch);
            inflightBatch = null;
        } finally {
            flushing.set(false);
        }
    }

    private void writeBatch(List<JsonObject> batch) throws Exception {
        if (batch.isEmpty()) return;

        // Requêtes paramétrées
        String insertSQL = "INSERT INTO metrics (type, rate, unit, timestamp) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = clickhouseConn.prepareStatement(insertSQL)) {
            for (JsonObject metric : batch) {
                ps.setString(1, metric.getString("type"));
                // "rate" peut être Double ou Long selon le type, on normalise en Double
                Object rate = metric.getValue("rate");
                ps.setDouble(2, rate instanceof Number ? ((Number) rate).doubleValue() : 0.0);
                ps.setString(3, metric.getString("unit", ""));
                ps.setLong(4, metric.getLong("timestamp"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private Map<String, String> buildHudMetrics() {
        Map<String, String> metrics = new LinkedHashMap<>();

        metrics.put("SYSTEM_RAM", latestHudMetrics.getOrDefault("SYSTEM_RAM", lastSystemRam + " %"));
        metrics.put("SYSTEM_CPU", latestHudMetrics.getOrDefault("SYSTEM_CPU", lastSystemCpu + " %"));
        metrics.put("FLOW_AGG_RATE", latestHudMetrics.getOrDefault("FLOW_AGG_RATE", lastFlowAggRate + " packets/s"));
        metrics.put("INGEST.received", latestHudMetrics.getOrDefault("INGEST.received", String.valueOf(lastIngestReceived.get())));
        metrics.put("INGEST.queue", latestHudMetrics.getOrDefault("INGEST.queue", String.valueOf(lastIngestQueue.get())));
        metrics.put("CLICKHOUSE.flushes", latestHudMetrics.getOrDefault("CLICKHOUSE.flushes", String.valueOf(lastFlushCount.get())));

        Map<String, String> sorted = new TreeMap<>(latestHudMetrics);
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            metrics.putIfAbsent(e.getKey(), e.getValue());
        }

        return metrics;
    }

    // =====================================================================
// HUD console minimaliste (ANSI), version robuste — à mettre tel quel
// =====================================================================
private static final class ConsoleHud {
    private static final String ESC = "\u001B[";
    private static final String SAVE = ESC + "s";
    private static final String RESTORE = ESC + "u";
    private static final String HIDE_CURSOR = ESC + "?25l";
    private static final String SHOW_CURSOR = ESC + "?25h";
    private static final String RESET = ESC + "0m";
    private static final String INVERSE = ESC + "7m";
    private static final String DIM = ESC + "2m";

    private final int boxWidth;
    private final int boxHeight;
    private volatile int lastLeft = -1;
    private volatile int lastTop = -1;
    private volatile int lastTerminalWidth = 200;

    ConsoleHud(int boxWidth, int boxHeight) {
        this.boxWidth = boxWidth;
        this.boxHeight = boxHeight;
        // Cache le curseur (évite scintillement)
        System.err.print(HIDE_CURSOR);
        System.err.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(this::restore));
    }

    void restore() {
        System.err.print(SHOW_CURSOR);
        System.err.flush();
    }

    private int detectTerminalWidth() {
        String columns = System.getenv("COLUMNS");
        if (columns != null && columns.matches("\\d+")) {
            int value = Integer.parseInt(columns);
            if (value > 20) {
                lastTerminalWidth = value;
                return value;
            }
        }

        // Essayez d'abord stty (fiable en TTY, ne lit pas stdin)
        try {
            Process p = new ProcessBuilder("sh", "-c", "stty size < /dev/tty").redirectErrorStream(true).start();
            p.waitFor();
            String out = new String(p.getInputStream().readAllBytes()).trim();
            if (out.matches("\\d+\\s+\\d+")) {
                int value = Integer.parseInt(out.split("\\s+")[1]);
                if (value > 20) {
                    lastTerminalWidth = value;
                    return value;
                }
            }
        } catch (Exception ignored) {}

        return lastTerminalWidth;
    }

    private void move(int row, int col) {
        System.err.print(ESC + row + ";" + col + "H");
    }

    private void drawBox(int top, int left) {
        String horiz = "─".repeat(Math.max(0, boxWidth - 2));
        // haut
        move(top, left);
        System.err.print("┌" + horiz + "┐");
        // côtés
        for (int i = 1; i < boxHeight - 1; i++) {
            move(top + i, left);
            System.err.print("│");
            move(top + i, left + boxWidth - 1);
            System.err.print("│");
        }
        // bas
        move(top + boxHeight - 1, left);
        System.err.print("└" + horiz + "┘");
    }

    private void clearInside(int top, int left) {
        // Nettoie l'intérieur du cadre (remplissage d'espaces)
        for (int i = 1; i < boxHeight - 1; i++) {
            move(top + i, left + 1);
            System.err.print(" ".repeat(boxWidth - 2));
        }
    }

    private String fitInside(String text) {
        int innerWidth = boxWidth - 2;
        if (text == null) {
            text = "";
        }
        if (text.length() > innerWidth) {
            return text.substring(0, innerWidth);
        }
        return text + " ".repeat(innerWidth - text.length());
    }

    private void clearBoxArea(int top, int left) {
        for (int i = 0; i < boxHeight; i++) {
            move(top + i, left);
            System.err.print(" ".repeat(boxWidth));
        }
    }

    void render(Map<String, String> metrics) {
        // Sauver la position du curseur pour ne pas perturber les logs
        System.err.print(SAVE);

        int cols = detectTerminalWidth();
        int left = Math.max(1, cols - boxWidth - 1);
        int top = 1;

        if (lastLeft != -1 && (lastLeft != left || lastTop != top)) {
            clearBoxArea(lastTop, lastLeft);
        }

        if (lastLeft != left || lastTop != top) {
            drawBox(top, left);
            lastLeft = left;
            lastTop = top;
        } else {
            drawBox(top, left);
        }

        // Efface l'intérieur et réécrit tout à neuf pour éviter les restes
        clearInside(top, left);

        // Titre
        move(top + 1, left + 1);
        System.err.print(INVERSE + fitInside("  AUT25 • METRICS") + RESET);

        // Heure
        move(top + 2, left + 1);
        String ts = java.time.LocalTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
        System.err.print(DIM + fitInside("  " + ts) + RESET);

        // Contenu
        int line = top + 4;
        for (var e : metrics.entrySet()) {
            if (line >= top + boxHeight - 1) break;
            move(line, left + 2);
            String txt = String.format("%-22s %s", e.getKey() + ":", e.getValue());
            int contentWidth = boxWidth - 4;
            if (txt.length() > contentWidth) {
                txt = txt.substring(0, contentWidth);
            }
            System.err.print(txt + " ".repeat(contentWidth - txt.length()));
            line++;
        }

        System.err.flush();
        // Restaure la position du curseur : les logs continuent là où ils étaient
        System.err.print(RESTORE);
        System.err.flush();
    }
}


}
