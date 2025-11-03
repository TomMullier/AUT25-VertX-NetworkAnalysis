package com.aut25.vertx.api.routes;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.Main;
import com.aut25.vertx.api.WebServerVerticle;
import com.aut25.vertx.utils.Colors;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class PcapRoute {
        /* -------------------------------- Variables ------------------------------- */
        private static final Logger logger = LoggerFactory.getLogger(PcapRoute.class);

        private final Vertx vertx;
        private final Main mainVerticle;

        /* ------------------------------- Constructor ------------------------------ */
        public PcapRoute(Vertx vertx, Main mainVerticle) {
                this.vertx = vertx;
                this.mainVerticle = mainVerticle;
        }

        /* --------------------------------- Routes --------------------------------- */
        /**
         * Mount the network routes to the given router.
         * 
         * @param router The Vert.x router to mount the routes on.
         */
        public void mount(Router router) {
                router.get("/api/pcapInfo").handler(this::getPcapInfos);
        }

        /* --------------------------------- Methods -------------------------------- */
        /**
         * Handle get pcap infos request.
         * 
         * @param ctx The routing context.
         */
        private void getPcapInfos(RoutingContext ctx) {
                try {
                        // Récupère les paramètres globaux depuis le config
                        SharedData sharedData = vertx.sharedData();
                        LocalMap<String, Object> settings = sharedData.getLocalMap("config");

                        String activeFile = null;
                        if (settings != null) {
                                JsonObject settingsPcap = (JsonObject) settings.get("pcap");
                                if (settingsPcap != null) {
                                        activeFile = settingsPcap.getString("file-path", null);
                                        // Ne garder que le nom du fichier
                                        if (activeFile != null) {
                                                activeFile = activeFile.substring(
                                                                activeFile.lastIndexOf('/') + 1);
                                        }
                                }
                        }

                        // Récupère la liste des fichiers PCAP
                        String dataDirPath = "src/main/resources/data";
                        File dataDir = new File(dataDirPath);
                        JsonArray filesArray = new JsonArray();

                        if (dataDir.exists() && dataDir.isDirectory()) {
                                File[] files = dataDir.listFiles(
                                                (dir, name) -> name.toLowerCase().endsWith(".pcap"));
                                if (files != null) {
                                        for (File file : files) {
                                                filesArray.add(file.getName());
                                        }
                                }
                        }

                        // Crée la réponse combinée
                        JsonObject response = new JsonObject()
                                        .put("files", filesArray)
                                        .put("activePcapFile", activeFile);

                        ctx.response()
                                        .putHeader("Content-Type", "application/json")
                                        .end(response.encode());

                } catch (Exception e) {
                        ctx.response()
                                        .setStatusCode(500)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject()
                                                        .put("error", e.getMessage())
                                                        .encode());

                }
        }
}
