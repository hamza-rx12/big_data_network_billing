package me.hamza.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class MongoConfig {
    private static final String MONGO_URI = "mongodb://mongo:27017";
    private static final String DATABASE = "mydatabase";

    public static Sink<String> createMongoSink(String collection) {
        return new Sink<String>() {
            @Override
            public SinkWriter<String> createWriter(Sink.InitContext context) {
                return new SinkWriter<String>() {
                    private transient MongoClient mongoClient;
                    private transient MongoCollection<Document> mongoCollection;

                    @Override
                    public void write(String element, Context context) {
                        if (mongoClient == null) {
                            mongoClient = MongoClients.create(MONGO_URI);
                            MongoDatabase database = mongoClient.getDatabase(DATABASE);
                            mongoCollection = database.getCollection(collection);
                        }
                        Document doc = Document.parse(element);
                        mongoCollection.insertOne(doc);
                    }

                    @Override
                    public void flush(boolean endOfInput) {
                        // No-op as we're writing directly to MongoDB
                    }

                    @Override
                    public void close() {
                        if (mongoClient != null) {
                            mongoClient.close();
                        }
                    }
                };
            }
        };
    }
}