#include <jni.h>
#include <stdlib.h>
#include "com_aut25_vertx_NDPIWrapper.h"
#include "ndpi_api.h"

#ifndef NULL
#define NULL ((void *)0)
#endif

// Global nDPI module
struct ndpi_detection_module_struct *ndpi_mod = NULL;

// Initialise nDPI et retourne un flow par défaut
JNIEXPORT jlong JNICALL Java_com_aut25_vertx_NDPIWrapper_init(JNIEnv *env, jobject obj)
{
        ndpi_mod = ndpi_init_detection_module(NULL);
        if (!ndpi_mod)
                return 0;

        ndpi_load_protocols_file(ndpi_mod, NULL);

        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)malloc(sizeof(struct ndpi_flow_struct));
        if (!flow)
        {
                ndpi_exit_detection_module(ndpi_mod);
                ndpi_mod = NULL;
                return 0;
        }

        return (jlong)flow;
}

// Crée un nouveau flow séparé
JNIEXPORT jlong JNICALL Java_com_aut25_vertx_NDPIWrapper_createFlow(JNIEnv *env, jobject obj)
{
        struct ndpi_flow_struct *flow = ndpi_flow_malloc(sizeof(struct ndpi_flow_struct));
        if (!flow)
                return 0;
        memset(flow, 0, sizeof(struct ndpi_flow_struct));
        return (jlong)flow;
}

// Analyse un paquet pour un flow donné
JNIEXPORT jstring JNICALL Java_com_aut25_vertx_NDPIWrapper_analyzePacket(JNIEnv *env, jobject obj,
                                                                         jbyteArray packet,
                                                                         jlong ts,
                                                                         jlong flow_ptr)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (!ndpi_mod || !flow)
                return (*env)->NewStringUTF(env, "nDPI not initialized");

        jbyte *buf = (*env)->GetByteArrayElements(env, packet, NULL);
        int length = (*env)->GetArrayLength(env, packet);

        ndpi_protocol detected = ndpi_detection_process_packet(ndpi_mod, flow,
                                                               (const u_int8_t *)buf,
                                                               (u_int16_t)length,
                                                               (uint64_t)ts,
                                                               NULL);

        (*env)->ReleaseByteArrayElements(env, packet, buf, 0);

        const char *proto_name = "Unknown";

        if (detected.proto.app_protocol != NDPI_PROTOCOL_UNKNOWN)
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.app_protocol);
        else if (detected.proto.master_protocol != NDPI_PROTOCOL_UNKNOWN)
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.master_protocol);

        return (*env)->NewStringUTF(env, proto_name);
}

// Libère un flow
JNIEXPORT void JNICALL Java_com_aut25_vertx_NDPIWrapper_cleanup(JNIEnv *env, jobject obj, jlong flow_ptr)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (flow)
                free(flow);

        if (ndpi_mod)
        {
                ndpi_exit_detection_module(ndpi_mod);
                ndpi_mod = NULL;
        }
}
