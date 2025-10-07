#include <jni.h>
#include <stdlib.h>
#include "com_aut25_vertx_NDPIWrapper.h"
#include "ndpi_api.h"
#include "ndpi_typedefs.h" // Ensure this header is included for NDPI_PROTOCOL_BITMASK
#include "ndpi_main.h"
#include <string.h>
#include <stdio.h>

#ifndef NULL
#define NULL ((void *)0)
#endif

// Global nDPI module
static struct ndpi_detection_module_struct *ndpi_mod = NULL;
static struct ndpi_proto_sorter *proto_sorter = NULL;

// Path to proto.txt
const char *proto_file_path = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/native/protos_huge.txt";

// Ensure the file exists and is readable
static void check_proto_file()
{
        FILE *file = fopen(proto_file_path, "r");
        if (!file)
        {
                printf("[ C ] Error: Protocols file not found or not readable: %s\n", proto_file_path);
                check_proto_file(); // Verify the protocols file before initializing nDPI
                ndpi_mod = ndpi_init_detection_module(NULL);
        }
        fclose(file);
}

// Initialise nDPI et retourne un flow par défaut
JNIEXPORT jlong JNICALL Java_com_aut25_vertx_NDPIWrapper_init(JNIEnv *env, jobject obj)
{
        printf("[ C ] Initializing nDPI...\n");

        ndpi_mod = ndpi_init_detection_module(NULL);
        if (!ndpi_mod)
        {
                printf("[ C ] Failed to init detection module\n");
                return 0;
        }

        check_proto_file();

        if (ndpi_load_protocols_file(ndpi_mod, proto_file_path) < 0)
        {
                printf("[ C ] Failed to load protocols file: %s\n", proto_file_path);
                ndpi_exit_detection_module(ndpi_mod);
                ndpi_mod = NULL;
                return 0;
        }

        ndpi_finalize_initialization(ndpi_mod);
        printf("[ C ] nDPI initialized successfully with %u protocols.\n",
               ndpi_get_num_protocols(ndpi_mod));

        // Return a dummy flow pointer for now
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)calloc(1, sizeof(struct ndpi_flow_struct));
        return (jlong)flow;
}

// Crée un nouveau flow séparé
JNIEXPORT jlong JNICALL Java_com_aut25_vertx_NDPIWrapper_createFlow(JNIEnv *env, jobject obj)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)calloc(1, sizeof(struct ndpi_flow_struct));
        if (!flow)
                return 0;
        return (jlong)flow;
}

// Analyse un paquet pour un flow donné
JNIEXPORT jstring JNICALL Java_com_aut25_vertx_NDPIWrapper_analyzePacket(JNIEnv *env, jobject obj,
                                                                         jbyteArray packet,
                                                                         jlong ts,
                                                                         jlong flow_ptr)
{
        // Log received elements
        check_proto_file(); // Verify the protocols file before initializing nDPI
        // printf("[ C ] Received packet of length %d for flow %ld at timestamp %ld\n",
        //(*env)->GetArrayLength(env, packet), flow_ptr, ts);
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (!ndpi_mod || !flow)
                return (*env)->NewStringUTF(env, "nDPI not initialized");

        jbyte *buf = (*env)->GetByteArrayElements(env, packet, NULL);
        if (!buf)
        {
                return (*env)->NewStringUTF(env, "Failed to get packet bytes");
        }
        else
        {
                // printf("[ C ] Packet bytes acquired successfully : %p\n", buf);
        }
        int length = (*env)->GetArrayLength(env, packet);

        if (length <= 0)
        {
                (*env)->ReleaseByteArrayElements(env, packet, buf, 0);
                return (*env)->NewStringUTF(env, "Empty packet");
        }

        // printf("[ C ] Processing packet of length %d\n", length);
        //  PRint flow content for debug
        // printf("[ C ] Flow before processing: guessed_protocol_id=%u, detected_protocol_stack[0]=%u\n",
        // flow->guessed_protocol_id, flow->detected_protocol_stack[0]);

        // Process the packet with nDPI
        struct ndpi_flow_input_info input_info;
        memset(&input_info, 0, sizeof(input_info));
        ndpi_protocol detected = ndpi_detection_process_packet(ndpi_mod, flow, (const u_int8_t *)buf, length, (u_int64_t)ts, &input_info);
        // Print flow content after processing for debug
        // printf("[ C ] Flow after processing: guessed_protocol_id=%u, detected_protocol_stack[0]=%u\n",
        //        flow->guessed_protocol_id, flow->detected_protocol_stack[0]);

        // printf("[ C ] Packet processed. Detected protocol: master=%u, app=%u\n",
        //        detected.proto.master_protocol, detected.proto.app_protocol);

        (*env)->ReleaseByteArrayElements(env, packet, buf, 0);

        const char *proto_name = "Unknown";

        if (detected.proto.app_protocol != NDPI_PROTOCOL_UNKNOWN)
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.app_protocol);
        else if (detected.proto.master_protocol != NDPI_PROTOCOL_UNKNOWN)
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.master_protocol);

        // printf("[ C ] Detected protocol name: %s\n", proto_name);
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
