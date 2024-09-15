# syntax=docker.io/docker/dockerfile:1.7-labs

# Build stage
FROM eclipse-temurin:17-jdk-jammy AS base

WORKDIR /unitycatalog

# Copy the only necessary build folders, for docker caching optimization
COPY --parents build/ project/ examples/ server/ api/ bin/ version.sbt build.sbt ./

# Use the ./build/sbt script to build the application and unpack the tarball
RUN ./build/sbt createTarball \
    && cd target \
    && tar -xvf unitycatalog-*.tar.gz

# Runtime stage with JRE for reduced image size
FROM eclipse-temurin:17-jre-jammy AS runtime

# Copy the extracted tarball target and relevant folders
COPY --from=base --parents \
    /unitycatalog/bin/ \
    /unitycatalog/examples/ \
    /unitycatalog/target/ \
    /unitycatalog/server/ \
    /unitycatalog/

COPY entrypoint.sh /unitycatalog/

# Create a uc user with read and execute permissions and ownership of the ./etc directory
RUN groupadd --system ucuser \
    && useradd --system --disabled-password --gid ucuser ucuser \
    # Service is r-x by default
    && chmod -R 554 /unitycatalog \
    # However, target/ is rwx
    && chmod -R 774 /unitycatalog/target/ \
    # Service is owned by ucuser
    && chown -R ucuser:ucuser /unitycatalog


USER ucuser

EXPOSE 8080 8081

WORKDIR /unitycatalog

ENTRYPOINT ["./entrypoint.sh"]
