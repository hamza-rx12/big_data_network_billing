FROM bitnami/spark:3.5.6

USER root

# Install wget
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Copy the installation script
COPY install-mongo-connector.sh /opt/bitnami/spark/install-mongo-connector.sh

# Make the script executable
RUN chmod +x /opt/bitnami/spark/install-mongo-connector.sh

# Run the installation script
RUN /opt/bitnami/spark/install-mongo-connector.sh

# Switch back to non-root user
USER 1001 