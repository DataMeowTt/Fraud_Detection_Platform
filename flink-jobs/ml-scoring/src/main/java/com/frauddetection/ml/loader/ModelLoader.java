package com.frauddetection.ml.loader;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;

import java.io.InputStream;

public class ModelLoader {

    public static byte[] load(String endpoint, String accessKey, String secretKey,
                              String bucket, String objectKey) throws Exception {
        MinioClient client = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
        try (InputStream is = client.getObject(
                GetObjectArgs.builder().bucket(bucket).object(objectKey).build())) {
            return is.readAllBytes();
        }
    }
}
