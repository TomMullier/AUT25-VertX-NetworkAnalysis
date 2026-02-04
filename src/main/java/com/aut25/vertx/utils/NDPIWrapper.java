package com.aut25.vertx.utils;

public class NDPIWrapper {

    static {
        // System.load("/home/tom/IMAGIN_LAB/MULLIER_Tom/AUT25-VertX-NetworkAnalysis/native/libndpi_jni.so");
        System.load("/home/au76890@ens.ad.etsmtl.ca/Documents/AUT25-VertX-NetworkAnalysis/native/libndpi_jni.so");
    }

    // Initialise nDPI et retourne le pointeur vers le contexte nDPI
    public native long init();

    // Crée un nouveau flow séparé et retourne le pointeur
    public native long createFlow();

    // Analyse un paquet pour un flow donné
    public native String analyzePacket(byte[] packet, long ts, long flowPtr);

    // Libère un flow
    public native void cleanup(long flowPtr);

    public native int getFlowRiskMask(long flowPtr);

    public native int getFlowRiskScore(long flowPtr);

    public native String getFlowRiskLabel(long flowPtr);

    public native String getFlowRiskSeverity(long flowPtr);

}
