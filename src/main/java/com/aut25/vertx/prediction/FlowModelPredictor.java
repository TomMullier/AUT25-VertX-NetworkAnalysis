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
    static {

        String path = "IDS_Training/dataset_10-03-26/xgb_full_pipeline.pmml";

        try (InputStream is =
                FlowModelPredictor.class.getClassLoader().getResourceAsStream(path)) {

            if (is == null) {
                throw new RuntimeException("PMML not found in classpath: " + path);
            }

            logger.info("[ FLOWMODELPREDICTOR ]            Loading PMML model from: " + path);

            evaluator = new LoadingModelEvaluatorBuilder()
                    .load(is)
                    .build();

            evaluator.verify();

            logger.info("[ FLOWMODELPREDICTOR ]            PMML model loaded successfully");

        Map<String, Object> arguments = new HashMap<>();

        List<InputField> inputFields = evaluator.getInputFields();
        } catch (Exception e) {
            e.printStackTrace();   // IMPORTANT pour voir la vraie erreur
            throw new RuntimeException("Failed to load PMML model", e);
        }
    }


    public static String predict(Map<String, Object> flowFeatures) {

        Map<String, Object> arguments = new HashMap<>();

        List<InputField> inputFields = evaluator.getInputFields();

        

        for (InputField inputField : inputFields) {

            String name = inputField.getName();

            Object rawValue = flowFeatures.get(name);
            if (rawValue == null) {
                rawValue = 0.0;
            }

            if (rawValue instanceof Double && ((Double) rawValue).isNaN()) {
                rawValue = 0.0;
            }

            Object value = inputField.prepare(rawValue);

            arguments.put(name, value);
        }

        Map<String, ?> results = evaluator.evaluate(arguments);

        TargetField targetField = evaluator.getTargetFields().get(0);

        Object prediction = results.get(targetField.getName());

        Integer resolved = resolvePredictionLabel(prediction, results);
        if (resolved != null) {
            return resolved == 1 ? "MALICIOUS" : "BENIGN";
        }

        if (prediction instanceof Number) {
            return ((Number) prediction).intValue() == 1 ? "MALICIOUS" : "BENIGN";
        }

        return prediction.toString() == "1" ? "MALICIOUS" : "BENIGN";
    }

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
            
}
