package com.aut25.vertx.prediction;

import org.jpmml.evaluator.*;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowModelPredictor {

    private static Evaluator evaluator;
    public static Logger logger = LoggerFactory.getLogger(FlowModelPredictor.class);
    // added by me  ===============================
        private static final Map<String, Double> PROTOCOL_MAP = new HashMap<>();
        static {
            PROTOCOL_MAP.put("TCP", 6.0);
            PROTOCOL_MAP.put("UDP", 17.0);
            PROTOCOL_MAP.put("ICMP", 1.0);
            PROTOCOL_MAP.put("ICMPv6", 58.0);
            PROTOCOL_MAP.put("SCTP", 132.0);
            // add others if needed
        }

        // ===============================
    static {

        // String path = "IDS_Training/dataset_10-03-26/xgb_full_pipeline.pmml"; // old code
        String path = "IDS_Training/xgb_model.pmml"; // changed by me

        
        
        
        try (InputStream is =
                FlowModelPredictor.class.getClassLoader().getResourceAsStream(path)) {

            if (is == null) {
                throw new RuntimeException("PMML not found in classpath: " + path);
            }

            logger.info("[ FLOWMODELPREDICTOR ]            Loading model from: " + path); // deleted PMML

            evaluator = new LoadingModelEvaluatorBuilder()
                    .load(is)
                    .build();

            evaluator.verify();

            logger.info("[ FLOWMODELPREDICTOR ]             Model loaded successfully"); // deleted PMML

        Map<String, Object> arguments = new HashMap<>();

        List<InputField> inputFields = evaluator.getInputFields();
        } catch (Exception e) {
            e.printStackTrace();   // IMPORTANT pour voir la vraie erreur
            throw new RuntimeException("Failed to load model", e); // deleted PMML
        }
    }


    public static String predict(Map<String, Object> flowFeatures) {

        Map<String, Object> arguments = new HashMap<>();

        List<InputField> inputFields = evaluator.getInputFields();

        

        for (InputField inputField : inputFields) {

            String name = inputField.getName();

            Object rawValue = flowFeatures.get(name);

            // added by me for debugging ===============================
            // System.out.println(name + " = " + rawValue);

            // if(rawValue == null){
            //     System.out.println("MISSING FEATURE -> " + name);
            // }
            // System.out.println("FLOW FEATURES:");
            // flowFeatures.forEach((k,v) ->
            //     System.out.println(k + " = " + v)
            // );
            // System.out.println("EXPECTED PMML FEATURES:");
            // System.out.println(flowFeatures);
            // for(InputField f : evaluator.getInputFields()){
            //     System.out.println(f.getName());
            // }
            // ========================
            if (rawValue == null) {
                rawValue = 0.0;
            }

            // =============== added by me 

            if ("protocol".equals(name) && rawValue instanceof String) {
                rawValue = PROTOCOL_MAP.getOrDefault(
                    ((String) rawValue).toUpperCase(), 0.0
                );
            }
            // ===============================


            if (rawValue instanceof Double && ((Double) rawValue).isNaN()) {
                rawValue = 0.0;
            }

            Object value = inputField.prepare(rawValue);

            arguments.put(name, value);
        }

        Map<String, ?> results = evaluator.evaluate(arguments);
        // System.out.println(results); //added by me for debugging to show the probabilities
        TargetField targetField = evaluator.getTargetFields().get(0);

        Object prediction = results.get(targetField.getName());

        // added by me =====================
        // Extract all class probabilities
        double p0 = getProbability(results, 0);
    double p1 = getProbability(results, 1);
    double p2 = getProbability(results, 2);
    double p3 = getProbability(results, 3);

    logger.info("[ ML PROBS ] p0(benign)={} p1(dos)={} p2(bruteforce)={} p3(sqli)={}",
        String.format("%.3f", p0),
        String.format("%.3f", p1),
        String.format("%.3f", p2),
        String.format("%.3f", p3));
    

    // Get hard label
    int hardLabel = 0;
    if (prediction instanceof Computable) {
        Object computed = ((Computable) prediction).getResult();
        if (computed instanceof Number) {
            hardLabel = ((Number) computed).intValue();
        } else if (computed != null) {
            try { hardLabel = Integer.parseInt(computed.toString()); }
            catch (NumberFormatException ignored) {}
        }
    }

    // Normal traffic floor: p0 >= 0.583 always
    // Attack traffic:       p0 =  0.416 (observed)
    // Safe threshold: 0.50 — sits cleanly between the two zones
    //
    // ADDITIONAL guard: attack class must ALSO be the dominant non-benign class
    // AND must exceed a minimum signal threshold to avoid noise
    final double BENIGN_THRESHOLD   = 0.50;
    final double DOS_MIN_SIGNAL     = 0.15; // p1 must be meaningfully elevated
    final double BRUTEFORCE_MIN_SIGNAL = 0.10;
    final double SQLI_MIN_SIGNAL    = 0.45; // p3 reaches 0.40 on normal traffic, so needs higher bar

    if (p0 >= BENIGN_THRESHOLD) {
        return "BENIGN";
    }

    // p0 < 0.50 — genuine attack zone, now pick the class
    // Use hard label first (most reliable when model is confident)
    if (hardLabel != 0) {
        return switch (hardLabel) {
            case 1 -> "DOS";
            case 2 -> "BRUTEFORCE";
            case 3 -> "SQLI";
            default -> "BENIGN";
        };
    }

    // Fallback: probability-based with minimum signal guards
    if (p1 > p2 && p1 > p3 && p1 >= DOS_MIN_SIGNAL)        return "DOS";
    if (p2 > p1 && p2 > p3 && p2 >= BRUTEFORCE_MIN_SIGNAL) return "BRUTEFORCE";
    if (p3 > p1 && p3 > p2 && p3 >= SQLI_MIN_SIGNAL)       return "SQLI";

    return "BENIGN";
}

private static double getProbability(Map<String, ?> results, int classLabel) {
    Object p = results.get("probability(" + classLabel + ")");
    return p instanceof Number ? ((Number) p).doubleValue() : 0.0;
}

// ======================================================
        

    //     // Integer resolved = resolvePredictionLabel(prediction, results);
    //     // if (resolved != null) {
    //     //     return resolved != 0 ? "MALICIOUS" : "BENIGN";
    //     // }

    //     if (prediction instanceof Number) {
    //         return ((Number) prediction).intValue() == 1 ? "MALICIOUS" : "BENIGN";
    //     }

    //     return "1".equals(prediction.toString()) ? "MALICIOUS" : "BENIGN";
    // }

    /**
     * Resolve model output for classification results such as ProbabilityDistribution.
     */
    private static Integer resolvePredictionLabel(Object prediction, Map<String, ?> results) {
        if (prediction == null) {
            return null;
        }

        // JPMML often wraps classification outputs into a Computable object.
        if (prediction instanceof Computable) {
            Object computed = ((Computable) prediction).getResult();
            if (computed instanceof Number) {
                return ((Number) computed).intValue();
            }
            if (computed != null) {
                try {
                    return Integer.parseInt(computed.toString());
                } catch (NumberFormatException ignored) {
                    // Fall through to probability-based fallback.
                }
            }
        }

        // Fallback: derive the label from explicit probability outputs.
        Object p1 = results.get("probability(1)");
        Object p0 = results.get("probability(0)");
        if (p1 instanceof Number && p0 instanceof Number) {
            return ((Number) p1).doubleValue() >= ((Number) p0).doubleValue() ? 1 : 0;
        }

        return null;
    }

    /**
     * Retourne la liste des features attendues par le PMML
     */
    public static List<String> getExpectedFeatures() {
        List<String> names = new ArrayList<>();
        for (InputField f : evaluator.getInputFields()) {
            names.add(f.getName());
        }
        return names;
    }

    /**
     * Filtre une Map pour ne garder que les features attendues par le PMML
     */
    public static Map<String, Object> filterFeatures(Map<String, Object> flowFeatures) {
        List<String> expected = getExpectedFeatures();
        Map<String, Object> filtered = new HashMap<>();
        for (String key : expected) {
            filtered.put(key, flowFeatures.getOrDefault(key, 0.0));
        }
        return filtered;
    }

    // ========= added by me for debugging ==========================
    /**
 * Diagnostic: compares what the PMML model expects against what the flow
 * actually provides, so we can see which features are silently defaulting to 0.0.
 */
public static void debugFeatureAlignment(Map<String, Object> flowFeatures) {
    List<String> expected = getExpectedFeatures();
    java.util.Set<String> actualKeys = flowFeatures.keySet();

    logger.info("==== FEATURE ALIGNMENT DEBUG ====");
    logger.info("Expected by PMML: {} | Present in flow map: {}", expected.size(), actualKeys.size());

    List<String> missingInFlow = new ArrayList<>();
    for (String key : expected) {
        if (!actualKeys.contains(key)) {
            missingInFlow.add(key);
        }
    }

    List<String> unusedInModel = new ArrayList<>();
    for (String key : actualKeys) {
        if (!expected.contains(key)) {
            unusedInModel.add(key);
        }
    }

    logger.info("--- Expected by model but MISSING from flow (defaults to 0.0) [{}] ---", missingInFlow.size());
    for (String k : missingInFlow) logger.info("   MISSING -> {}", k);

    logger.info("--- Present in flow but UNUSED by model [{}] ---", unusedInModel.size());
    for (String k : unusedInModel) logger.info("   UNUSED  -> {}", k);

    logger.info("==== END FEATURE ALIGNMENT DEBUG ====");
}

// ======================================

            
}
