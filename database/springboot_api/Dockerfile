# Use an official OpenJDK runtime as the base image
FROM eclipse-temurin:17-jdk-jammy

# Set the working directory in the container
WORKDIR /app

# Copy the JAR file into the container
COPY target/springboot_api-0.0.1-SNAPSHOT.jar app.jar
COPY ./wait-for-it.sh  wait-for-it.sh
RUN chmod +x ./wait-for-it.sh

# Run the application when the container starts
CMD ["./wait-for-it.sh", "mongo:27017", "--","java", "-jar", "app.jar"]