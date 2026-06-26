# Spark 4.1.x with Unity Catalog (built from repo source), Delta Lake, and S3 (MinIO).
# syntax=docker/dockerfile:1

ARG SPARK_IMAGE=apache/spark:4.1.2-scala2.13-java17-python3-ubuntu

FROM eclipse-temurin:17-jdk-jammy AS uc-builder

ARG SBT_VERSION=1.10.7
ENV SBT_OPTS="-Dsbt.io.implicit.relative.glob.conversion=allow -Xmx2g"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
         | tar xz -C /usr/local \
    && ln -sf /usr/local/sbt/bin/sbt /usr/local/bin/sbt \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Minimal source tree for client + hadoop + spark connector publish.
COPY version.sbt build.sbt ./
COPY project project
COPY api api
COPY clients/java clients/java
COPY connectors/hadoop connectors/hadoop
COPY connectors/spark connectors/spark
COPY dev/checkstyle-config.xml dev/checkstyle-suppressions.xml dev/

RUN sbt -DsparkVersion=4.1 -batch client/generate \
    && sbt -DsparkVersion=4.1 -batch "client/package; hadoop/package; spark/package"

RUN mkdir -p /tmp/uc-m2/io/unitycatalog/unitycatalog-client/0.5.0-SNAPSHOT \
             /tmp/uc-m2/io/unitycatalog/unitycatalog-hadoop/0.5.0-SNAPSHOT \
             /tmp/uc-m2/io/unitycatalog/unitycatalog-spark_4.1_2.13/0.5.0-SNAPSHOT \
    && cp clients/java/target/unitycatalog-client-0.5.0-SNAPSHOT.jar /tmp/uc-m2/io/unitycatalog/unitycatalog-client/0.5.0-SNAPSHOT/ \
    && cp connectors/hadoop/target/unitycatalog-hadoop-0.5.0-SNAPSHOT.jar /tmp/uc-m2/io/unitycatalog/unitycatalog-hadoop/0.5.0-SNAPSHOT/ \
    && cp connectors/spark/target/scala-2.13/unitycatalog-spark_4.1_2.13-0.5.0-SNAPSHOT.jar /tmp/uc-m2/io/unitycatalog/unitycatalog-spark_4.1_2.13/0.5.0-SNAPSHOT/

ARG DELTA_VERSION=4.2.0
ARG HADOOP_AWS_VERSION=3.4.2
ARG UC_SPARK_VERSION=0.5.0-SNAPSHOT

COPY docker/spark/pom.xml /tmp/spark-uc-deps/pom.xml
RUN apt-get update \
    && apt-get install -y --no-install-recommends maven \
    && mvn -q install:install-file \
         -DgroupId=io.unitycatalog \
         -DartifactId=unitycatalog-client \
         -Dversion=0.5.0-SNAPSHOT \
         -Dpackaging=jar \
         -Dfile=/tmp/uc-m2/io/unitycatalog/unitycatalog-client/0.5.0-SNAPSHOT/unitycatalog-client-0.5.0-SNAPSHOT.jar \
    && mvn -q install:install-file \
         -DgroupId=io.unitycatalog \
         -DartifactId=unitycatalog-hadoop \
         -Dversion=0.5.0-SNAPSHOT \
         -Dpackaging=jar \
         -Dfile=/tmp/uc-m2/io/unitycatalog/unitycatalog-hadoop/0.5.0-SNAPSHOT/unitycatalog-hadoop-0.5.0-SNAPSHOT.jar \
    && mvn -q install:install-file \
         -DgroupId=io.unitycatalog \
         -DartifactId=unitycatalog-spark_4.1_2.13 \
         -Dversion=0.5.0-SNAPSHOT \
         -Dpackaging=jar \
         -Dfile=/tmp/uc-m2/io/unitycatalog/unitycatalog-spark_4.1_2.13/0.5.0-SNAPSHOT/unitycatalog-spark_4.1_2.13-0.5.0-SNAPSHOT.jar \
    && mvn -q -f /tmp/spark-uc-deps/pom.xml \
         -Duc.spark.version="${UC_SPARK_VERSION}" \
         -Ddelta.version="${DELTA_VERSION}" \
         -Dhadoop.aws.version="${HADOOP_AWS_VERSION}" \
         -DoutputDirectory=/tmp/spark-jars \
         dependency:copy-dependencies \
    && apt-get purge -y maven \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/spark-uc-deps

FROM ${SPARK_IMAGE}

USER root

# Replace any previously copied UC connector jars from an older image layer.
RUN rm -f /opt/spark/jars/unitycatalog-*.jar

COPY --from=uc-builder /tmp/spark-jars/*.jar /opt/spark/jars/

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/spark/scripts/ /opt/spark/uc-scripts/
RUN chmod +x /opt/spark/uc-scripts/*.sh

ENV UC_SERVER_URI=http://host.docker.internal:8080 \
    UC_CATALOG=catA \
    UC_AUTH_TOKEN_FILE=/etc/uc/token.txt \
    MINIO_ENDPOINT=http://minio:9000

WORKDIR /opt/spark

USER spark

ENTRYPOINT ["/opt/spark/uc-scripts/entrypoint.sh"]
