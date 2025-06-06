# Use an official OpenJDK runtime as the base image
FROM eclipse-temurin:17-jdk-jammy

# Set the working directory in the container
WORKDIR /app

# Copy the JAR file into the container
COPY target/database-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

# Run the application when the container starts
CMD ["java", "-jar", "app.jar"]