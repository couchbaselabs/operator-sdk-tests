FROM maven:3.8.1-jdk-11 AS build
COPY src src
COPY pom.xml .
RUN mvn clean compile dependency:copy-dependencies assembly:assembly

FROM openjdk:11-jre-slim
COPY --from=build target/sdktest-1.0.jar /usr/local/lib/sdktest.jar
ENTRYPOINT [ "java","-jar","/usr/local/lib/sdktest.jar" ]
