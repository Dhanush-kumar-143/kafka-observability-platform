# Stage 1: download the Prometheus JMX exporter java agent
FROM alpine:3.19 AS jmx
ARG JMX_EXPORTER_VERSION=0.20.0
RUN apk add --no-cache curl \
 && mkdir -p /jmx \
 && curl -fsSL -o /jmx/jmx_prometheus_javaagent.jar \
    "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar" \
 && chmod 644 /jmx/jmx_prometheus_javaagent.jar

# Stage 2: Apache Kafka (KRaft) with the agent attached
FROM apache/kafka:3.7.0
COPY --from=jmx /jmx/jmx_prometheus_javaagent.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY kafka-jmx.yml /opt/jmx/kafka-jmx.yml
# Exposes broker JMX metrics in Prometheus format on :7071
ENV KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/kafka-jmx.yml"
EXPOSE 9092 7071
