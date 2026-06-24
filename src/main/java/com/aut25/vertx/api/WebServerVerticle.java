package com.aut25.vertx.api;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.core.shareddata.SharedData;

import at.favre.lib.crypto.bcrypt.BCrypt; // added by me 

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import io.vertx.core.json.JsonArray;

import com.aut25.vertx.utils.Colors;

import at.favre.lib.crypto.bcrypt.BCrypt;

import com.aut25.vertx.Main;
import com.aut25.vertx.api.routes.*;

// ====added by me ================
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.Row;
// ===================================

public class WebServerVerticle extends AbstractVerticle {
        // ====added by me
        private JDBCPool jdbcClient;
        // ===============      
        private static final Logger logger = LoggerFactory.getLogger(WebServerVerticle.class);
        private final Set<ServerWebSocket> clients = ConcurrentHashMap.newKeySet();
        private final Main mainVerticle;
        private JsonObject config;

        public WebServerVerticle(Main mainVerticle) {
                this.mainVerticle = mainVerticle;
        }


        // =============start method ===================================


        @Override
        public void start(Promise<Void> startPromise) {
                // ====added by me================
                jdbcClient = JDBCPool.pool(
                vertx,
                new io.vertx.jdbcclient.JDBCConnectOptions()
                        .setJdbcUrl("jdbc:sqlite:database.db"),
                new io.vertx.sqlclient.PoolOptions()
                );

        jdbcClient.getConnection()
.onSuccess(conn -> {

    System.out.println("Connected to SQLite");

conn.query("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT UNIQUE,
        password TEXT
    )
""")
    .execute()
    .onSuccess(res -> {
        System.out.println("Users table ready");
        conn.close();
    })
    .onFailure(err -> {
        System.out.println("TABLE ERROR");
        err.printStackTrace();
        conn.close();
    });

})
.onFailure(err -> {
    System.out.println("CONNECTION ERROR");
    err.printStackTrace();
});
// ===============================================
        

                logger.info(Colors.BLUE + "[ WEBSERVER ]                     Starting WebSocket and HTTP server"
                                + Colors.RESET);

                try {
                        LocalMap<String, Object> map = vertx.sharedData().getLocalMap("config");
                        config = new JsonObject(map);
                        int port = config.getInteger("http.port", 8888);

                        Router router = Router.router(vertx);
                        router.route().handler(BodyHandler.create());

                        // Routes API
                        new SettingsRoute(vertx, mainVerticle).mount(router);
                        new PcapRoute(vertx, mainVerticle).mount(router);
                        new NetworkRoute(vertx, mainVerticle).mount(router);
                        new UtilsRoute(vertx, mainVerticle).mount(router);
                        

//                         // =====================================added by me
//                         // rest of your code...
//         router.post("/signup").handler(ctx -> {
//                  System.out.println(" SIGNUP REQUEST RECEIVED");

//         JsonObject body = ctx.getBodyAsJson();

//         String name = body.getString("name");
//         String email = body.getString("email");
//         String password = body.getString("password");

//         String sql = "INSERT INTO users(name, email, password) VALUES (?, ?, ?)";

//         jdbcClient.preparedQuery(sql)
//                 .execute(Tuple.of(name, email, password))
//         .onSuccess(res -> {
//             ctx.json(new JsonObject().put("success", true));
//         })
//         .onFailure(err -> {
//             ctx.json(new JsonObject()
//                 .put("success", false)
//                 .put("error", "User already exists"));
//         });
// });


// // router.post("/login").handler(ctx -> {
// //         System.out.println("LOGIN REQUEST RECEIVED");

// //     JsonObject body = ctx.getBodyAsJson();

// //     String email = body.getString("email");
// //     String password = body.getString("password");

// //     String sql = "SELECT * FROM users WHERE email = ? AND password = ?";

// //     jdbcClient.preparedQuery(sql)
// //         .execute(Tuple.of(email, password))
// //         .onSuccess(res -> {

// //             if (res.iterator().hasNext()) {

// //                 Row row = res.iterator().next();

// //                 ctx.json(new JsonObject()
// //                     .put("success", true)
// //                     .put("name", row.getString("name")));

// //             } else {
// //                 ctx.json(new JsonObject().put("success", false));
// //             }
// //         });
// // });
// // // ==================================================


//                         // 1. FORCE LOGIN FIRST
//                         router.get("/").handler(ctx -> {
//                         ctx.reroute("/login.html");
//                         });

//                         // 2. STATIC FILES
//                         router.route("/*")
//                         .handler(StaticHandler.create("webroot")
//                         .setCachingEnabled(false));


// // =============== added by me =========================
// // Auth guard — must come BEFORE the static handler
// router.get("/index.html").handler(ctx -> {
//     String cookie = ctx.request().getHeader("Cookie");
//     if (cookie != null && cookie.contains("fv_auth=1")) {
//         ctx.next(); // authenticated, let static handler serve it
//     } else {
//         ctx.redirect("/login.html");
//     }
// });

// // Update login route to SET the cookie on success
// router.post("/login").handler(ctx -> {
//     JsonObject body = ctx.getBodyAsJson();
//     String email = body.getString("email");
//     String password = body.getString("password");
//     String sql = "SELECT * FROM users WHERE email = ? AND password = ?";
//     jdbcClient.preparedQuery(sql)
//         .execute(Tuple.of(email, password))
//         .onSuccess(res -> {
//             if (res.iterator().hasNext()) {
//                 Row row = res.iterator().next();
//                 ((RoutingContext) ctx.response()
//                     .putHeader("Set-Cookie", "fv_auth=1; Path=/; HttpOnly; SameSite=Strict"))
//                     .json(new JsonObject()
//                         .put("success", true)
//                         .put("name", row.getString("name")));
//             } else {
//                 ctx.json(new JsonObject().put("success", false));
//             }
//         });
// });

// // Add logout endpoint
// router.get("/logout").handler(ctx -> {
//     ((RoutingContext) ctx.response()
//         .putHeader("Set-Cookie", "fv_auth=; Path=/; Max-Age=0; HttpOnly"))
//         .redirect("/login.html");
// });

// =====================================added by me  work well =========================
// router.post("/signup").handler(ctx -> {
//     System.out.println("SIGNUP REQUEST RECEIVED");
//     JsonObject body = ctx.getBodyAsJson();
//     String name = body.getString("name");
//     String email = body.getString("email");
//     String password = body.getString("password");
//     String sql = "INSERT INTO users(name, email, password) VALUES (?, ?, ?)";
//     jdbcClient.preparedQuery(sql)
//         .execute(Tuple.of(name, email, password))
//         .onSuccess(res -> ctx.json(new JsonObject().put("success", true)))
//         .onFailure(err -> ctx.json(new JsonObject()
//             .put("success", false)
//             .put("error", "User already exists")));
// });

// router.post("/login").handler(ctx -> {
//     System.out.println("LOGIN REQUEST RECEIVED");
//     JsonObject body = ctx.getBodyAsJson();
//     String email = body.getString("email");
//     String password = body.getString("password");
//     String sql = "SELECT * FROM users WHERE email = ? AND password = ?";
//     jdbcClient.preparedQuery(sql)
//         .execute(Tuple.of(email, password))
//         .onSuccess(res -> {
//             if (res.iterator().hasNext()) {
//                 Row row = res.iterator().next();
//                 // ✅ No casting — just chain directly on ctx.response()
//                 ctx.response()
//                     .putHeader("Set-Cookie", "fv_auth=1; Path=/; HttpOnly; SameSite=Strict")
//                     .putHeader("Content-Type", "application/json")
//                     .end(new JsonObject()
//                         .put("success", true)
//                         .put("name", row.getString("name"))
//                         .encode());
//             } else {
//                 ctx.json(new JsonObject().put("success", false));
//             }
//         });
// });



// ==================================            work well 


// ==================== added by me new code with encryption ======================


// // SIGNUP — hash before storing
// router.post("/signup").handler(ctx -> {
//     System.out.println("SIGNUP REQUEST RECEIVED");
//     JsonObject body = ctx.getBodyAsJson();
//     String name     = body.getString("name");
//     String email    = body.getString("email");
//     String password = body.getString("password");

//     // ✅ Hash the password with cost factor 12
//     String hashed = BCrypt.withDefaults().hashToString(12, password.toCharArray());

//     String sql = "INSERT INTO users(name, email, password) VALUES (?, ?, ?)";
//     jdbcClient.preparedQuery(sql)
//         .execute(Tuple.of(name, email, hashed))
//         .onSuccess(res -> ctx.json(new JsonObject().put("success", true)))
//         .onFailure(err -> ctx.json(new JsonObject()
//             .put("success", false)
//             .put("error", "User already exists")));
// });

// // LOGIN — fetch by email only, then verify hash
// router.post("/login").handler(ctx -> {
//     System.out.println("LOGIN REQUEST RECEIVED");
//     JsonObject body = ctx.getBodyAsJson();
//     String email    = body.getString("email");
//     String password = body.getString("password");

//     // ✅ Fetch by email only (never compare plain passwords in SQL)
//     String sql = "SELECT * FROM users WHERE email = ?";
//     jdbcClient.preparedQuery(sql)
//         .execute(Tuple.of(email))
//         .onSuccess(res -> {
//             if (res.iterator().hasNext()) {
//                 Row row = res.iterator().next();
//                 String storedHash = row.getString("password");

//                 // ✅ Verify the submitted password against the stored hash
//                 BCrypt.Result result = BCrypt.verifyer()
//                     .verify(password.toCharArray(), storedHash);

//                 if (result.verified) {
//                     ctx.response()
//                         .putHeader("Set-Cookie", "fv_auth=1; Path=/; HttpOnly; SameSite=Strict")
//                         .putHeader("Content-Type", "application/json")
//                         .end(new JsonObject()
//                             .put("success", true)
//                             .put("name", row.getString("name"))
//                             .encode());
//                 } else {
//                     ctx.json(new JsonObject().put("success", false));
//                 }
//             } else {
//                 // ✅ Same response whether email missing or password wrong
//                 //    (prevents user enumeration attacks)
//                 ctx.json(new JsonObject().put("success", false));
//             }
//         })
//         .onFailure(err -> ctx.json(new JsonObject()
//             .put("success", false)
//             .put("error", "Server error")));
// });




// // ===============================

// router.get("/logout").handler(ctx -> {
//     // ✅ No casting — just chain directly
//     ctx.response()
//         .putHeader("Set-Cookie", "fv_auth=; Path=/; Max-Age=0; HttpOnly")
//         .setStatusCode(302)
//         .putHeader("Location", "/login.html")
//         .end();
// });

// // ✅ Auth guard — BEFORE static handler
// router.get("/index.html").handler(ctx -> {
//     String cookie = ctx.request().getHeader("Cookie");
//     if (cookie != null && cookie.contains("fv_auth=1")) {
//         ctx.next();
//     } else {
//         ctx.redirect("/login.html");
//     }
// });

// // ✅ Redirect / to login — BEFORE static handler
// router.get("/").handler(ctx -> ctx.redirect("/login.html"));

// // ✅ Static handler — MUST be last
// router.route("/*")
//     .handler(StaticHandler.create("webroot")
//     .setCachingEnabled(false));

//     ========================


// =================================== new code ===================
// At the top of your start() method, initialize the session store
// (add this right after jdbcClient setup)
LocalMap<String, String> sessions = vertx.sharedData().getLocalMap("sessions");

// SIGNUP — hash before storing
router.post("/signup").handler(ctx -> {
    System.out.println("SIGNUP REQUEST RECEIVED");
    JsonObject body = ctx.getBodyAsJson();
    String name     = body.getString("name");
    String email    = body.getString("email");
    String password = body.getString("password");

    String hashed = BCrypt.withDefaults().hashToString(12, password.toCharArray());

    String sql = "INSERT INTO users(name, email, password) VALUES (?, ?, ?)";
    jdbcClient.preparedQuery(sql)
        .execute(Tuple.of(name, email, hashed))
        .onSuccess(res -> ctx.json(new JsonObject().put("success", true)))
        .onFailure(err -> ctx.json(new JsonObject()
            .put("success", false)
            .put("error", "User already exists")));
});

// LOGIN — create a real server-side session token
router.post("/login").handler(ctx -> {
    System.out.println("LOGIN REQUEST RECEIVED");
    JsonObject body = ctx.getBodyAsJson();
    String email    = body.getString("email");
    String password = body.getString("password");

    String sql = "SELECT * FROM users WHERE email = ?";
    jdbcClient.preparedQuery(sql)
        .execute(Tuple.of(email))
        .onSuccess(res -> {
            if (res.iterator().hasNext()) {
                Row row = res.iterator().next();
                String storedHash = row.getString("password");

                BCrypt.Result result = BCrypt.verifyer()
                    .verify(password.toCharArray(), storedHash);

                if (result.verified) {
                    // ✅ Generate a unique session token
                    String sessionToken = java.util.UUID.randomUUID().toString();
                    String userName = row.getString("name");

                    // ✅ Store token → username in server-side session map
                    LocalMap<String, String> sess = vertx.sharedData().getLocalMap("sessions");
                    sess.put(sessionToken, userName);

                    System.out.println("Session created: " + sessionToken + " for " + userName);

                    ctx.response()
                        // ✅ Cookie holds the token, NOT a hardcoded "1"
                        .putHeader("Set-Cookie", "fv_session=" + sessionToken + "; Path=/; HttpOnly; SameSite=Lax")
                        .putHeader("Content-Type", "application/json")
                        .end(new JsonObject()
                            .put("success", true)
                            .put("name", userName)
                            .encode());
                } else {
                    ctx.json(new JsonObject().put("success", false));
                }
            } else {
                ctx.json(new JsonObject().put("success", false));
            }
        })
        .onFailure(err -> ctx.json(new JsonObject()
            .put("success", false)
            .put("error", "Server error")));
});

// LOGOUT — remove session from server-side store
router.get("/logout").handler(ctx -> {
    String cookieHeader = ctx.request().getHeader("Cookie");
    if (cookieHeader != null) {
        // Extract the session token from the cookie header
        for (String part : cookieHeader.split(";")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("fv_session=")) {
                String token = trimmed.substring("fv_session=".length());
                // ✅ Remove from server-side store — token is now dead
                LocalMap<String, String> sess = vertx.sharedData().getLocalMap("sessions");
                sess.remove(token);
                System.out.println("Session invalidated: " + token);
                break;
            }
        }
    }
    ctx.response()
        .putHeader("Set-Cookie", "fv_session=; Path=/; Max-Age=0; HttpOnly")
        .setStatusCode(302)
        .putHeader("Location", "/login.html")
        .end();
});

// AUTH GUARD — validates token against server-side session store
router.get("/index.html").handler(ctx -> {
    String cookieHeader = ctx.request().getHeader("Cookie");
    String token = null;

    if (cookieHeader != null) {
        for (String part : cookieHeader.split(";")) {
            String trimmed = part.trim();
            if (trimmed.startsWith("fv_session=")) {
                token = trimmed.substring("fv_session=".length());
                break;
            }
        }
    }

    LocalMap<String, String> sess = vertx.sharedData().getLocalMap("sessions");
    boolean valid = token != null && sess.containsKey(token);

    System.out.println("AUTH GUARD — token: " + token + " | valid: " + valid);

    if (valid) {
        ctx.next();
    } else {
        ctx.redirect("/login.html");
    }
});

// Redirect / to login
router.get("/").handler(ctx -> ctx.redirect("/login.html"));

// Static handler — MUST be last
router.route("/*")
    .handler(StaticHandler.create("webroot")
    .setCachingEnabled(false));

//     ================================================

// ===========================================================


                        HttpServer server = vertx.createHttpServer();


                        

                        // Gestion WebSocket
                        server.webSocketHandler(ws -> {
                                if (!"/".equals(ws.path())) {
                                        ws.reject();
                                        return;
                                }

                                logger.info("[WS]                              New client connected: {}",
                                                ws.remoteAddress());
                                clients.add(ws);

                                ws.closeHandler(v -> {
                                        logger.info("[WS]                              Client disconnected: {}",
                                                        ws.remoteAddress());
                                        clients.remove(ws);
                                });

                                ws.exceptionHandler(err -> {
                                        logger.error("[WS]                              Error on connection {}: {}",
                                                        ws.remoteAddress(), err.getMessage());
                                        clients.remove(ws);
                                });
                        });

                        vertx.eventBus().consumer("flows.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "flow");
                                broadcast(data);
                        });

                        // vertx.eventBus().consumer("packets.data", msg -> {
                        // if (!(msg.body() instanceof JsonObject))
                        // return;
                        // JsonObject data = ((JsonObject) msg.body()).copy();
                        // data.put("type", "packet");
                        // broadcast(data);
                        // });

                        vertx.eventBus().consumer("currentFlows.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "currentFlow");
                                broadcast(data);
                        });

                        vertx.eventBus().consumer("malformedPackets.data", msg -> {
                                if (!(msg.body() instanceof JsonObject))
                                        return;
                                JsonObject data = ((JsonObject) msg.body()).copy();
                                data.put("type", "malformedPacket");
                                broadcast(data);
                        });

                        vertx.eventBus().consumer("metrics.core", message -> {
                                JsonObject data = (JsonObject) message.body();
                                // FLOW_AGGREGATION_RATE
                                // SYSTEM_CPU
                                // SYSTEM_RAM
                                broadcast(data);
                        });

                        server.requestHandler(router)
                                        .listen(port)
                                        .onSuccess(s -> {
                                                logger.info(Colors.MAGENTA
                                                                + "[ WEBSERVER ]                     Started on port "
                                                                + port + Colors.RESET);
                                                if (!startPromise.future().isComplete())
                                                        startPromise.complete();
                                        })
                                        .onFailure(err -> {
                                                logger.error("[ WEBSERVER ]                     Failed to start: ",
                                                                err);
                                                if (!startPromise.future().isComplete())
                                                        startPromise.fail(err);
                                        });

                } catch (Exception e) {
                        logger.error("[ WEBSERVER ]                     Critical exception during start()", e);
                        if (!startPromise.future().isComplete())
                                startPromise.fail(e);
                }
        }

        /**
         * Diffuse un message JSON à tous les clients WebSocket connectés.
         */
        private void broadcast(JsonObject data) {
                String message = data.encode();
                clients.removeIf(ws -> ws == null || ws.isClosed());
                for (ServerWebSocket ws : clients) {
                        try {
                                ws.writeTextMessage(message);
                        } catch (Exception e) {
                                logger.warn("[WS]                              Unable to send to {}: {}",
                                                ws.remoteAddress(), e.getMessage());
                                clients.remove(ws);
                        }
                }
        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ WEBSERVER ]                     WebSocket and HTTP server stopped!"
                                + Colors.RESET);
                clients.forEach(ws -> {
                        if (!ws.isClosed())
                                ws.close();
                });
                clients.clear();
        }

}
