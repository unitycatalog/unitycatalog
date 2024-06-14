FROM openjdk:24-slim

WORKDIR /opt/app

COPY server/target/unitycatalog-server*.jar unitycatalog-server.jar
COPY examples/cli/target/unitycatalog-cli*.jar unitycatalog-cli.jar
COPY etc .

RUN cd /opt/app

ENTRYPOINT ["java", "-cp", "unitycatalog-server.jar", "io.unitycatalog.server.UnityCatalogServer"]
