# Use an official OpenJDK runtime as the base image
FROM confluentinc/cp-spark:7.5.0-java17

# Set the working directory in the container
WORKDIR /app

# Copy the JAR file into the container
COPY target/consumer-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

# Run the application when the container starts
CMD [ "spark-submit", "--master", "spark://spark-master:7077", "app.jar" ]