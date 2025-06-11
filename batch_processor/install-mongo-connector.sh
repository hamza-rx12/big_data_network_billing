#!/bin/bash

# Download MongoDB Spark Connector
wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.1/mongo-spark-connector_2.12-10.2.1.jar -O /opt/bitnami/spark/jars/mongo-spark-connector_2.12-10.2.1.jar

# Download MongoDB Java Driver (required by the connector)
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar -O /opt/bitnami/spark/jars/mongodb-driver-sync-4.11.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar -O /opt/bitnami/spark/jars/bson-4.11.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar -O /opt/bitnami/spark/jars/mongodb-driver-core-4.11.1.jar

# Set proper permissions
chmod 644 /opt/bitnami/spark/jars/*.jar

echo "MongoDB Spark Connector installed successfully" 