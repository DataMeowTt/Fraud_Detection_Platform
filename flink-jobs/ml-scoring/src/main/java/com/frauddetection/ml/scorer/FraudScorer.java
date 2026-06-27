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
        String modelKey  = System.getenv().getOrDefault("MINIO_MODEL_KEY",      "fraud_detection_model.json");
        threshold        = Float.parseFloat(
                           System.getenv().getOrDefault("ML_FRAUD_THRESHOLD",   "0.9688"));

        byte[] modelBytes = ModelLoader.load(endpoint, accessKey, secretKey, bucket, modelKey);
        booster = XGBoost.loadModel(modelBytes);
    }

    public float score(float[] features) throws Exception {
        DMatrix matrix = new DMatrix(features, 1, NUM_FEATURES, Float.NaN);
        return booster.predict(matrix)[0][0];
    }

    public boolean isFraud(float score) {
        return score >= threshold;
    }
}
