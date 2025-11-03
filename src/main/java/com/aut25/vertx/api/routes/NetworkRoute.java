package com.aut25.vertx.api.routes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aut25.vertx.Main;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class NetworkRoute {
        /* -------------------------- Variable Declaration -------------------------- */
        private static final Logger logger = LoggerFactory.getLogger(NetworkRoute.class);

        private final Vertx vertx;
        private final Main mainVerticle;

        /* ------------------------------- Constructor ------------------------------ */
        public NetworkRoute(Vertx vertx, Main mainVerticle) {
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
                router.get("/api/networkInfo").handler(this::getNetworkInfos);
        }

        /* --------------------------------- Methods -------------------------------- */
        /**
         * Handle get network infos request.
         * 
         * @param ctx The routing context.
         */
        private void getNetworkInfos(RoutingContext ctx) {
                try {
                        // Get list of network interfaces available on the system
                        java.util.List<String> interfaces = new java.util.ArrayList<>();
                        java.util.Enumeration<java.net.NetworkInterface> nets = java.net.NetworkInterface
                                        .getNetworkInterfaces();
                        while (nets.hasMoreElements()) {
                                java.net.NetworkInterface netIf = nets.nextElement();
                                if (netIf.isUp() && !netIf.isLoopback() && !netIf.isVirtual()) {
                                        interfaces.add(netIf.getName());
                                }
                        }

                        io.vertx.core.shareddata.LocalMap<String, Object> map_ = ctx.vertx()
                                        .sharedData().getLocalMap("config");
                        io.vertx.core.json.JsonObject config_ = new io.vertx.core.json.JsonObject(map_);

                        String activeInterface = null;
                        if (config_.containsKey("realtime")) {
                                io.vertx.core.json.JsonObject realtime = config_
                                                .getJsonObject("realtime");
                                activeInterface = realtime.getString("interface", null);
                        }

                        JsonObject response = new JsonObject()
                                        .put("interfaces", interfaces)
                                        .put("activeInterface", activeInterface);

                        ctx.response()
                                        .putHeader("Content-Type", "application/json")
                                        .end(response.encode());

                } catch (Exception e) {
                        ctx.response()
                                        .setStatusCode(500)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject().put("error", e.getMessage()).encode());
                }

        }
}
