package com.aut25.vertx;

public class NdpiFlowWrapper {
        public final long ndpiFlowPtr; // pointeur natif vers ndpi_flow_struct
        public long lastSeen;
        public String detectedProtocol = "UNKNOWN";

        public NdpiFlowWrapper(long ndpiFlowPtr) {
                this.ndpiFlowPtr = ndpiFlowPtr;
        }
}