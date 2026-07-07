FROM flink:1.20-java17

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop

RUN cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar \
      /opt/flink/plugins/s3-fs-hadoop/

RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.5/flink-metrics-prometheus-1.20.5.jar" \
    -o /opt/flink/lib/flink-metrics-prometheus-1.20.5.jar

# Not bundled in the base image — needed to select state.backend.type: rocksdb
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/1.20.5/flink-statebackend-rocksdb-1.20.5.jar" \
    -o /opt/flink/lib/flink-statebackend-rocksdb-1.20.5.jar