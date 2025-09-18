package com.aut25.vertx;

import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyseVerticle extends AbstractVerticle {

        private static final Logger logger = LoggerFactory.getLogger(AnalyseVerticle.class);

        @Override
        public void start() {
                logger.info(Colors.GREEN + "[ ANALYSE VERTICLE ] Starting AnalyseVerticle..." + Colors.RESET);
        }

        @Override
        public void stop() {
                logger.info(Colors.RED + "[ ANALYSE VERTICLE ] AnalyseVerticle stopped!" + Colors.RESET);
        }
}
