#include <jni.h>
#include "com_aut25_vertx_NDPIWrapper.h"
#include <ndpi_api.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Null definitions for missing constants
#ifndef NULL
#define NULL ((void *)0)
#endif

static struct ndpi_detection_module_struct *ndpi_mod = NULL;
static struct ndpi_flow_struct *ndpi_flow = NULL;

JNIEXPORT void JNICALL Java_com_aut25_vertx_NDPIWrapper_init(JNIEnv *env, jobject obj)
{
        // Initialisation du module de détection nDPI
        ndpi_mod = ndpi_init_detection_module(NULL); // <--- Ajout de NULL ici
        if (ndpi_mod == NULL)
        {
                fprintf(stderr, "Failed to initialize ndpi detection module\n");
        }
        if (!ndpi_mod)
        {
                fprintf(stderr, "❌ Failed to initialize nDPI detection module\n");
                return;
        }

        // Charger la configuration et les protocoles par défaut
        ndpi_load_protocols_file(ndpi_mod, NULL);

        // Créer une structure de flux (flow)
        ndpi_flow = ndpi_flow_malloc(SIZEOF_FLOW_STRUCT);
        if (!ndpi_flow)
        {
                fprintf(stderr, "❌ Failed to allocate nDPI flow structure\n");
                ndpi_exit_detection_module(ndpi_mod);
                ndpi_mod = NULL;
                return;
        }

        printf("✅ nDPI initialized successfully.\n");
}

JNIEXPORT jstring JNICALL Java_com_aut25_vertx_NDPIWrapper_analyzePacket(JNIEnv *env, jobject obj, jbyteArray packet)
{
        if (!ndpi_mod || !ndpi_flow)
                return (*env)->NewStringUTF(env, "nDPI not initialized");

        // Récupération du contenu du paquet depuis Java
        jbyte *buf = (*env)->GetByteArrayElements(env, packet, NULL);
        int length = (*env)->GetArrayLength(env, packet);

        // Analyse du paquet
        ndpi_protocol detected = ndpi_detection_process_packet(
            ndpi_mod,
            ndpi_flow,
            (const unsigned char *)buf,
            (unsigned short)length,
            0,
            NULL);

        (*env)->ReleaseByteArrayElements(env, packet, buf, 0);

        const char *proto_name = "Unknown";

        // 🔍 Détermination du protocole détecté
        if (detected.proto.app_protocol != NDPI_PROTOCOL_UNKNOWN)
        {
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.app_protocol);
        }
        else if (detected.proto.master_protocol != NDPI_PROTOCOL_UNKNOWN)
        {
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.master_protocol);
        }

        return (*env)->NewStringUTF(env, proto_name);
}

JNIEXPORT void JNICALL Java_com_aut25_vertx_NDPIWrapper_cleanup(JNIEnv *env, jobject obj)
{
        if (ndpi_flow)
        {
                free(ndpi_flow);
                ndpi_flow = NULL;
        }

        if (ndpi_mod)
        {
                ndpi_exit_detection_module(ndpi_mod);
                ndpi_mod = NULL;
        }

        printf("✅ nDPI cleaned up successfully.\n");
}