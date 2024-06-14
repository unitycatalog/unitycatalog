FROM openjdk:24-slim

WORKDIR /opt/app

COPY server/target/unitycatalog-server-assembly.jar unitycatalog-server.jar
COPY examples/cli/target/unitycatalog-cli-assembly.jar unitycatalog-cli.jar
COPY etc etc
COPY bin/uc-docker uc

ENV SERVER_PROPERTIES_FILE=/opt/app/etc/conf/server.properties
ENV SERVER_JOG4J_CONFIGURATION_FILE=/opt/app/etc/conf/server.log4j2.properties
ENV CLI_JOG4J_CONFIGURATION_FILE=/opt/app/etc/conf/cli.log4j2.properties

ENTRYPOINT ["java", "-cp", "unitycatalog-server.jar", "io.unitycatalog.server.UnityCatalogServer"]
