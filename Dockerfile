# syntax=docker/dockerfile:1.7
# One build stage for the whole reactor; the runtime stage picks a module by build arg.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
COPY common/pom.xml common/
COPY rider-request-service/pom.xml rider-request-service/
COPY driver-location-service/pom.xml driver-location-service/
COPY matching-service/pom.xml matching-service/
COPY loadgen/pom.xml loadgen/
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -DskipTests -Dspotless.check.skip=true dependency:resolve-plugins dependency:resolve || true
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -DskipTests -Dspotless.check.skip=true package

FROM eclipse-temurin:21-jre
ARG MODULE
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -Djava.security.egd=file:/dev/./urandom"
WORKDIR /app
COPY --from=build --chown=10001:10001 /src/${MODULE}/target/${MODULE}.jar /app/app.jar
USER 10001:10001
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
