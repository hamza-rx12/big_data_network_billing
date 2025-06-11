package me.hamza.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

public class MongoConfig {
    private static final String MONGO_URI = "mongodb://mongo:27017";
    private static final String DATABASE = "mydatabase";

    public static Map<String, String> createReadOptions(String collection) {
        Map<String, String> readOptions = new HashMap<>();
        readOptions.put("uri", MONGO_URI);
        readOptions.put("database", DATABASE);
        readOptions.put("collection", collection);
        return readOptions;
    }

    public static Map<String, String> createWriteOptions(String collection) {
        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put("uri", MONGO_URI);
        writeOptions.put("database", DATABASE);
        writeOptions.put("collection", collection);
        return writeOptions;
    }

    public static SparkSession createSparkSession() {
        return SparkSession.builder()
                .master("spark://spark-master:7077")
                .appName("MongoDB Batch Processor")
                .config("spark.jars.ivy", "/opt/bitnami/spark/jars")
                .config("spark.mongodb.read.connection.uri", MONGO_URI)
                .config("spark.mongodb.write.connection.uri", MONGO_URI)
                .config("spark.mongodb.read.database", DATABASE)
                .config("spark.mongodb.write.database", DATABASE)
                .getOrCreate();
    }
}