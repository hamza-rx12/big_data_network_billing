# Use an official OpenJDK runtime as the base image
# FROM eclipse-temurin:21-jdk-jammy
FROM confluentinc/cp-flink:1.18.1-cp3-java17-amd64

# Set the working directory in the container
# WORKDIR /app

# Copy the JAR file into the container
COPY target/consumer-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

# Run the application when the container starts
# CMD ["java", "-jar", "app.jar"]
CMD [ "flink", "run", "-m", "jobmanager:8081", "app.jar" ]