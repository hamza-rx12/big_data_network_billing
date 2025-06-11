# Use the Spark image as base
FROM spark-master-image

USER root

# Set working directory
WORKDIR /opt/bitnami/spark

# Copy the JAR file into the container
COPY target/batch_processor-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/bitnami/spark/jars/batch_processor.jar

# Switch back to non-root user
USER 1001

# Run the application using spark-submit
CMD ["spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--class", "me.hamza.Main", \
    "--conf", "spark.jars.ivy=/opt/bitnami/spark/jars", \
    "/opt/bitnami/spark/jars/batch_processor.jar"]