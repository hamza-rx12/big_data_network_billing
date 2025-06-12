# Use the Spark image as base
FROM spark-master-image

USER root

# Set working directory
WORKDIR /opt/bitnami/spark

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Switch back to non-root user
USER 1001

# Run the application using spark-submit
CMD ["spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--conf", "spark.jars.ivy=/opt/bitnami/spark/jars", \
    "main.py"] 