package com.aut25.vertx;

public class NDPIWrapper {

    static {
        System.load("/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/native/libndpi_jni.so");
    }

    // Initialise nDPI et retourne le pointeur vers le contexte nDPI
    public native long init();

    // Crée un nouveau flow séparé et retourne le pointeur
    public native long createFlow();

    // Analyse un paquet pour un flow donné
    public native String analyzePacket(byte[] packet, long ts, long flowPtr);

    // Libère un flow
    public native void cleanup(long flowPtr);

}
