package com.aut25.vertx;

public class NdpiWrapper {

        // Charger la lib JNI (nom choisi à l'étape de compilation)
        static {
                System.loadLibrary("ndpi_jni");
        }

        // // Exemple : initialiser le moteur nDPI
        // public native long init();

        // // Exemple : analyser un paquet (payload en bytes)
        // public native String processPacket(byte[] packetData, int packetLen);

        // // Exemple : libérer le moteur nDPI
        // public native void cleanup(long ndpiHandler);

        public static void main(String[] args) {
                NdpiWrapper ndpi = new NdpiWrapper();

                // long handler = ndpi.init();

                // byte[] dummyPacket = { /* données brutes d’un paquet Ethernet/IP */ };
                // String proto = ndpi.processPacket(dummyPacket, dummyPacket.length);

                // System.out.println("Protocole détecté : " + proto);

                // ndpi.cleanup(handler);
        }
}
