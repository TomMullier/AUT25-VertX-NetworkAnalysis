package com.aut25.vertx;

public class NDPIWrapper {

    static {
        System.load("/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/native/libndpi_jni.so");
    }

    public native void init();

    public native String analyzePacket(byte[] packet);

    public native void cleanup();

    public static void main(String[] args) {
        NDPIWrapper w = new NDPIWrapper();
        w.init();
        System.out.println("✅ JNI call to C init() successful!");
    }

}
