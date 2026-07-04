package com.frauddetection.ml.scorer;

import com.frauddetection.ml.loader.ModelLoader;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;

import java.io.Serializable;

public class FraudScorer implements Serializable {

    private static final int NUM_FEATURES = 9;

    private float threshold;
    private transient Booster booster;

    public void open() throws Exception {
        String endpoint  = System.getenv().getOrDefault("MINIO_ENDPOINT",       "http://minio:9000");
        String accessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY",     "minioadmin");
        String secretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY",     "minioadmin123");
        String bucket    = System.getenv().getOrDefault("MINIO_BUCKET",         "ml-models");
        String modelKey  = System.getenv().getOrDefault("MINIO_MODEL_KEY",      "model_detection.json");
        threshold        = Float.parseFloat(
                           System.getenv().getOrDefault("ML_FRAUD_THRESHOLD",   "0.9184"));

        byte[] modelBytes = ModelLoader.load(endpoint, accessKey, secretKey, bucket, modelKey);
        booster = XGBoost.loadModel(modelBytes);
    }

    public float[] scoreBatch(float[][] featuresBatch) throws Exception {
        int batchSize = featuresBatch.length;
        float[] flat = new float[batchSize * NUM_FEATURES];
        for (int i = 0; i < batchSize; i++) {
            System.arraycopy(featuresBatch[i], 0, flat, i * NUM_FEATURES, NUM_FEATURES);
        }

        DMatrix matrix = new DMatrix(flat, batchSize, NUM_FEATURES, Float.NaN);
        try {
            float[][] predictions = booster.predict(matrix);
            float[] scores = new float[batchSize];
            for (int i = 0; i < batchSize; i++) {
                scores[i] = predictions[i][0];
            }
            return scores;
        } finally {
            matrix.dispose();
        }
    }

    public boolean isFraud(float score) {
        return score >= threshold;
    }
}
