#include <jni.h>
#include <stdlib.h>
#include "com_aut25_vertx_NDPIWrapper.h"
#include "ndpi_api.h"
#include "ndpi_typedefs.h" // Ensure this header is included for NDPI_PROTOCOL_BITMASK
#include "ndpi_main.h"
#include <string.h>
#include <stdio.h>

/**
 * @brief Definition of NULL if not already defined
 *
 */
#ifndef NULL
#define NULL ((void *)0)
#endif

/**
 * @brief Definition of the nDPI detection module and protocol sorter
 *
 */
static struct ndpi_detection_module_struct *ndpi_mod = NULL;
static struct ndpi_proto_sorter *proto_sorter = NULL;

/**
 * @brief Definition of the protocol file path
 *
 */
const char *proto_file_path = "/home/tommullier/Documents/AUT25_Projet/AUT25-VertX-NetworkAnalysis/native/protos_huge.txt";

/**
 * @brief Check if the protocol file is accessible and initialize nDPI if it is.
 *
 */
static void check_proto_file()
{
        FILE *file = fopen(proto_file_path, "r");
        if (!file)
        {
                printf("[ C ] Error: Protocols file not found or not readable: %s\n", proto_file_path);
                ndpi_mod = ndpi_init_detection_module(NULL);
        }
        fclose(file);
}

/**
 * @brief Initializes the nDPI detection module.
 *
 * @param env   JNIEnv pointer
 * @param obj   Java object reference
 * @return JNIEXPORT jlong JNICALL
 */
JNIEXPORT jlong JNICALL Java_com_aut25_vertx_NDPIWrapper_init(JNIEnv *env, jobject obj)
{
        printf("\033[35m           [ C ]                             Initializing nDPI...\033[0m\n");

        // Initialize nDPI
        ndpi_mod = ndpi_init_detection_module(NULL);
        if (!ndpi_mod)
        {
                printf("\033[35m           [ C ]                             Failed to init detection module\033[0m\n");
                return 0;
        }

        // Check and load the protocol file
        check_proto_file();
        if (ndpi_load_protocols_file(ndpi_mod, proto_file_path) < 0)
        {
                printf("\033[35m           [ C ]                             Failed to load protocols file: %s\033[0m\n", proto_file_path);
                ndpi_exit_detection_module(ndpi_mod);
                ndpi_mod = NULL;
                return 0;
        }

        // Finalize nDPI initialization
        ndpi_finalize_initialization(ndpi_mod);
        printf("\033[35m           [ C ]                             nDPI initialized successfully with %u protocols.\033[0m\n",
               ndpi_get_num_protocols(ndpi_mod));

        // Create and return a default flow
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)calloc(1, sizeof(struct ndpi_flow_struct));
        return (jlong)flow;
}

/**
 * @brief Initializes a new flow structure.
 * @param env  JNIEnv pointer
 * @param obj  Java object reference
 * @return JNIEXPORT jlong JNICALL
 */
JNIEXPORT jlong JNICALL Java_com_aut25_vertx_NDPIWrapper_createFlow(JNIEnv *env, jobject obj)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)calloc(1, sizeof(struct ndpi_flow_struct));
        if (!flow)
                return 0;
        return (jlong)flow;
}

/**
 * @brief Analyzes a packet and detects its protocol.
 *
 * @param env   JNIEnv pointer
 * @param obj   Java object reference
 * @param packet The packet data to analyze
 * @param ts    The timestamp of the packet
 * @param flow_ptr Pointer to the flow structure
 * @return JNIEXPORT jstring JNICALL
 */
JNIEXPORT jstring JNICALL Java_com_aut25_vertx_NDPIWrapper_analyzePacket(JNIEnv *env, jobject obj,
                                                                         jbyteArray packet,
                                                                         jlong ts,
                                                                         jlong flow_ptr)
{
        check_proto_file(); // Verify the protocols file before initializing nDPI
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (!ndpi_mod || !flow)
                return (*env)->NewStringUTF(env, "nDPI not initialized");

        jbyte *buf = (*env)->GetByteArrayElements(env, packet, NULL);
        if (!buf)
                return (*env)->NewStringUTF(env, "Failed to get packet bytes");

        int length = (*env)->GetArrayLength(env, packet);

        if (length <= 0)
        {
                (*env)->ReleaseByteArrayElements(env, packet, buf, 0);
                return (*env)->NewStringUTF(env, "Empty packet");
        }

        // Process the packet with nDPI
        struct ndpi_flow_input_info input_info;
        memset(&input_info, 0, sizeof(input_info));
        ndpi_protocol detected = ndpi_detection_process_packet(ndpi_mod, flow, (const u_int8_t *)buf, length, (u_int64_t)ts, &input_info);

        // Release the byte array elements
        (*env)->ReleaseByteArrayElements(env, packet, buf, 0);

        // Get the protocol name
        const char *proto_name = "Unknown";
        if (detected.proto.app_protocol != NDPI_PROTOCOL_UNKNOWN)
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.app_protocol);
        else if (detected.proto.master_protocol != NDPI_PROTOCOL_UNKNOWN)
                proto_name = ndpi_get_proto_name(ndpi_mod, detected.proto.master_protocol);

        return (*env)->NewStringUTF(env, proto_name);
}

/**
 * @brief Cleans up the flow structure and releases resources.
 *
 * @param env  JNIEnv pointer
 * @param obj  Java object reference
 * @param flow_ptr Pointer to the flow structure
 * @return JNIEXPORT void JNICALL
 */
JNIEXPORT void JNICALL Java_com_aut25_vertx_NDPIWrapper_cleanup(JNIEnv *env, jobject obj, jlong flow_ptr)
{
        // Free the flow structure
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (flow)
        {
                free(flow);
        }
}

JNIEXPORT jint JNICALL Java_com_aut25_vertx_NDPIWrapper_getFlowRiskMask(JNIEnv *env, jobject obj, jlong flow_ptr)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (!ndpi_mod || !flow)
        {
                return -1; // Error: nDPI not initialized or invalid flow
        }
        return (jint)(flow->risk_mask);
}

JNIEXPORT jint JNICALL Java_com_aut25_vertx_NDPIWrapper_getFlowRiskScore(JNIEnv *env, jobject obj, jlong flow_ptr)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)flow_ptr;
        if (!ndpi_mod || !flow)
        {
                return -1; // Error: nDPI not initialized or invalid flow
        }
        return (jint)(flow->risk);
}

JNIEXPORT jstring JNICALL Java_com_aut25_vertx_NDPIWrapper_getFlowRiskLabel(JNIEnv *env, jobject obj, jlong flowPtr)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)(uintptr_t)flowPtr;
        if (flow == NULL)
                return (*env)->NewStringUTF(env, "Invalid flow");

        // printf("[ C ] Flow risk: %u\n", flow->risk);
        // printf("[ C ] Flow risk (binary): ");
        for (int i = sizeof(flow->risk) * 8 - 1; i >= 0; i--)
        {
                printf("%d", (flow->risk >> i) & 1);
        }
        // printf("[ C ] Risk enum value: %u\n", (ndpi_risk_enum)flow->risk);
        // printf("[ C ] Flow risk mask: %u\n", flow->risk_mask);

        char riskLabel[512] = "";
        uint64_t risk_bits[NDPI_MAX_RISK] = {0};
        for (int i = 0; i < NDPI_MAX_RISK; i++)
        {
                uint64_t mask = (uint64_t)1 << i; // <- décalage en 64 bits
                risk_bits[i] = (flow->risk & mask) ? 1 : 0;
        }
        for (int i = 0; i < NDPI_MAX_RISK; i++)
        {
                if (risk_bits[i])
                {
                        const char *label = ndpi_risk2str(i);
                        if (label && strlen(label) > 0)
                        {
                                if (strlen(riskLabel) > 0)
                                        strncat(riskLabel, ", ", sizeof(riskLabel) - strlen(riskLabel) - 1);
                                strncat(riskLabel, label, sizeof(riskLabel) - strlen(riskLabel) - 1);
                                // printf("[ C ] Detected risk: %s\n", label);
                                // printf("[ C ] Risk bit %d is set\n", i);
                        }
                }
        }

        if (strlen(riskLabel) == 0)
        {
                return (*env)->NewStringUTF(env, "No risk");
        }

        return (*env)->NewStringUTF(env, riskLabel);
}

const char *compareSeverities(const char *severity1, const char *severity2)
{
        if (!severity1 || !severity2)
        {
                return "Invalid severity";
        }

        if (strcmp(severity1, severity2) == 0)
        {
                return severity1; // Both severities are the same
        }

        if (strcmp(severity1, "Unknown severity") == 0)
        {
                return severity2; // Return the known severity
        }
        if (strcmp(severity2, "Unknown severity") == 0)
        {
                return severity1; // Return the known severity
        }

        const char *severityLevels[] = {"Low", "Medium", "High", "Severe", "Critical", "Emergency"};
        int sev1Index = -1, sev2Index = -1;

        for (int i = 0; i < sizeof(severityLevels) / sizeof(severityLevels[0]); i++)
        {
                if (strcmp(severity1, severityLevels[i]) == 0)
                        sev1Index = i;
                if (strcmp(severity2, severityLevels[i]) == 0)
                        sev2Index = i;
        }

        if (sev1Index == -1 || sev2Index == -1)
        {
                return "Invalid severity";
        }

        return (sev1Index > sev2Index) ? severityLevels[sev1Index] : severityLevels[sev2Index];
}

JNIEXPORT jstring JNICALL Java_com_aut25_vertx_NDPIWrapper_getFlowRiskSeverity(JNIEnv *env, jobject obj, jlong flowPtr)
{
        struct ndpi_flow_struct *flow = (struct ndpi_flow_struct *)(uintptr_t)flowPtr;
        if (!flow)
        {
                // printf("[ C ] getFlowRiskSeverity: Invalid flow pointer (NULL)\n");
                return (*env)->NewStringUTF(env, "Invalid flow");
        }

        char riskSeverity[512] = "";
        uint64_t risk_bits[NDPI_MAX_RISK] = {0};
        for (int i = 0; i < NDPI_MAX_RISK; i++)
        {
                uint64_t mask = (uint64_t)1 << i; // <- décalage en 64 bits
                risk_bits[i] = (flow->risk & mask) ? 1 : 0;
        }
        for (int i = 0; i < NDPI_MAX_RISK; i++)
        {
                if (risk_bits[i])
                {
                        ndpi_risk_info *riskSevInfo = ndpi_risk2severity(i);
                        if (riskSevInfo)
                        {
                                const char *severityStr = ndpi_severity2str(riskSevInfo->severity);
                                if (!severityStr || strlen(severityStr) == 0)
                                        severityStr = "Unknown severity";
                                // Keep only max severity
                                if (strlen(riskSeverity) == 0)
                                {
                                        strncpy(riskSeverity, severityStr, sizeof(riskSeverity) - 1);
                                }
                                else
                                {
                                        const char *higherSeverity = compareSeverities(riskSeverity, severityStr);
                                        strncpy(riskSeverity, higherSeverity, sizeof(riskSeverity) - 1);
                                }
                                // printf("[ C ] Detected risk severity: %s\n", severityStr);
                                // printf("[ C ] Risk bit %d is set\n", i);

                                // printf("[ C ] Current risk severity: %s\n", riskSeverity);
                        }
                }
        }

        // Log and return the severity string
        return (*env)->NewStringUTF(env, riskSeverity);
}
