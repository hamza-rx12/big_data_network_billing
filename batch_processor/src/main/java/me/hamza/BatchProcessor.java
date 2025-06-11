package me.hamza;

import me.hamza.config.MongoConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
// import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;

public class BatchProcessor {
        private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);
        private final SparkSession spark;
        private final ZonedDateTime endTime;
        private final ZonedDateTime startTime;
        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT;

        public String timestampSuffix() {
                return String.format("_%s_%s",
                                TIMESTAMP_FORMATTER.format(startTime).replace(":", "-"),
                                TIMESTAMP_FORMATTER.format(endTime).replace(":", "-"));
        }

        public BatchProcessor() {
                this.spark = MongoConfig.createSparkSession();
                this.endTime = ZonedDateTime.now(ZoneOffset.UTC);
                this.startTime = endTime.minus(1, ChronoUnit.MONTHS);
                logger.info("Processing records from {} to {}",
                                TIMESTAMP_FORMATTER.format(startTime),
                                TIMESTAMP_FORMATTER.format(endTime));
        }

        private Dataset<Row> filterByTimeRange(Dataset<Row> dataset) {
                return dataset.filter(
                                functions.col("timestamp").geq(functions.lit(TIMESTAMP_FORMATTER.format(startTime)))
                                                .and(functions.col("timestamp")
                                                                .lt(functions.lit(
                                                                                TIMESTAMP_FORMATTER.format(endTime)))));
        }

        public void processVoiceCalls() {
                logger.info("Processing voice calls data...");
                Map<String, String> readOptions = MongoConfig.createReadOptions("valid_records");

                Dataset<Row> voiceCalls = spark.read()
                                .format("mongodb")
                                .options(readOptions)
                                .load()
                                .filter("record_type = 1");

                // Apply time range filter
                voiceCalls = filterByTimeRange(voiceCalls);
                logger.info("Found {} voice call records in the time range", voiceCalls.count());

                // Calculate total call duration by caller
                Dataset<Row> totalCallDuration = voiceCalls
                                .groupBy("caller_id")
                                .agg(functions.sum("duration_sec").as("total_duration"))
                                .orderBy(functions.desc("total_duration"));

                // Calculate total calls by caller
                Dataset<Row> totalCalls = voiceCalls
                                .groupBy("caller_id")
                                .count()
                                .orderBy(functions.desc("count"));

                saveResults(totalCallDuration, "voice_calls_total_duration");
                saveResults(totalCalls, "voice_calls_total_count");
        }

        public void processSmsMessages() {
                logger.info("Processing SMS messages data...");
                Map<String, String> readOptions = MongoConfig.createReadOptions("valid_records");

                Dataset<Row> smsMessages = spark.read()
                                .format("mongodb")
                                .options(readOptions)
                                .load()
                                .filter("record_type = 2");

                // Apply time range filter
                smsMessages = filterByTimeRange(smsMessages);
                logger.info("Found {} SMS records in the time range", smsMessages.count());

                // Calculate total messages by sender
                Dataset<Row> totalMessages = smsMessages
                                .groupBy("sender_id")
                                .count()
                                .orderBy(functions.desc("count"));

                saveResults(totalMessages, "sms_total_messages");
        }

        public void processDataUsage() {
                logger.info("Processing data usage...");
                Map<String, String> readOptions = MongoConfig.createReadOptions("valid_records");

                Dataset<Row> dataUsage = spark.read()
                                .format("mongodb")
                                .options(readOptions)
                                .load()
                                .filter("record_type = 3");

                // Apply time range filter
                dataUsage = filterByTimeRange(dataUsage);
                logger.info("Found {} data usage records in the time range", dataUsage.count());

                // Calculate total data usage by user
                Dataset<Row> totalDataUsage = dataUsage
                                .groupBy("user_id")
                                .agg(functions.sum("data_volume_mb").as("total_data_mb"))
                                .orderBy(functions.desc("total_data_mb"));

                saveResults(totalDataUsage, "data_usage_total");
        }

        private void saveResults(Dataset<Row> results, String collectionName) {
                Map<String, String> writeOptions = MongoConfig.createWriteOptions("analytics_" + collectionName);
                results.write()
                                .format("mongodb")
                                .mode("overwrite")
                                .options(writeOptions)
                                .save();
                logger.info("Saved results to collection: analytics_{}", collectionName);
        }

        public void close() {
                if (spark != null) {
                        spark.close();
                }
        }

}