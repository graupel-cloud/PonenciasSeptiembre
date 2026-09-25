# syntax=docker/dockerfile:1
# Etapa 1: compilar y pasar las pruebas (la caché de ~/.m2 se reutiliza entre builds)
FROM maven:3.9.16-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
ARG SKIP_TESTS=false
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -Daether.transport.http.retryHandler.count=5 package -DskipTests=${SKIP_TESTS}

# Etapa 2: imagen de Flink con el job en usrlib (modo aplicación)
FROM flink:1.20.5-java17
COPY --from=build /src/target/reputacion-job.jar /opt/flink/usrlib/reputacion-job.jar
COPY start-jobmanager.sh /opt/flink/bin/start-jobmanager.sh
# El volumen de checkpoints se inicializa con estos permisos la primera vez
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints /opt/flink/usrlib \
    && chmod +x /opt/flink/bin/start-jobmanager.sh
