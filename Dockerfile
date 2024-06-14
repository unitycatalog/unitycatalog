FROM openjdk:24-slim

COPY server/target/unitycatalog-server*.jar /opt/app/unitycatalog-server.jar
COPY examples/cli/target/unitycatalog-cli*.jar /opt/app/unitycatalog-cli.jar
COPY etc /opt/app/etc

ENTRYPOINT ["java", "-cp", "/opt/app/unitycatalog-server.jar", "io.unitycatalog.server.UnityCatalogServer"]
