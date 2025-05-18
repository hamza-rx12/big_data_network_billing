# Use an official OpenJDK runtime as the base image
FROM eclipse-temurin:21-jdk-jammy

# Set the working directory in the container
WORKDIR /app

# Copy the JAR file into the container
COPY target/producer-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
COPY ./wait-for-it.sh  wait-for-it.sh
RUN chmod +x ./wait-for-it.sh

# Run the application when the container starts
CMD ["./wait-for-it.sh", "database:8080", "--", "java", "-jar", "app.jar"]