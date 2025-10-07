package com.aut25.vertx;

/**
 * Wrapper for managing nDPI flow pointers and associated metadata.
 */
public class NdpiFlowWrapper {
        public final long ndpiFlowPtr; // pointeur natif vers ndpi_flow_struct
        public long lastSeen;
        public String detectedProtocol = "UNKNOWN";

        public NdpiFlowWrapper(long ndpiFlowPtr) {
                this.ndpiFlowPtr = ndpiFlowPtr;
        }

        public void updateLastSeen(long timestamp) {
                this.lastSeen = timestamp;
        }

        public void setDetectedProtocol(String protocol) {
                this.detectedProtocol = protocol;
        }

        @Override
        public String toString() {
                return "NdpiFlowWrapper{" +
                                "ndpiFlowPtr=" + ndpiFlowPtr +
                                ", lastSeen=" + lastSeen +
                                ", detectedProtocol='" + detectedProtocol + '\'' +
                                '}';

        }

        public void display() {
                System.out.println(this.toString());
        }
}