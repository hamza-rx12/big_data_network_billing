package me.hamza;

import me.hamza.config.MongoConfig;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.udf;

import java.util.*;

public class RatingProcessor {
    private final SparkSession spark;

    public RatingProcessor() {
        this.spark = MongoConfig.createSparkSession();
    }

    public void processRatings() {
        // Read all required datasets
        Dataset<Row> customers = spark.read()
                .format("mongodb")
                .options(MongoConfig.createReadOptions("customers"))
                .load()
                .withColumn("ratePlanIdLong", col("ratePlanId").cast(DataTypes.LongType))
                .withColumnRenamed("_id", "customer_id");

        Dataset<Row> ratePlans = spark.read()
                .format("mongodb")
                .options(MongoConfig.createReadOptions("ratePlan"))
                .load()
                .withColumn("ratePlanIdLong", col("ratePlanId"));

        Dataset<Row> dataUsage = spark.read()
                .format("mongodb")
                .options(MongoConfig.createReadOptions("analytics_data_usage_total"))
                .load()
                .withColumnRenamed("user_id", "customer_id")
                .withColumn("data_usage", col("total_data_mb").cast(DataTypes.DoubleType));

        Dataset<Row> smsUsage = spark.read()
                .format("mongodb")
                .options(MongoConfig.createReadOptions("analytics_sms_total_messages"))
                .load()
                .withColumnRenamed("sender_id", "customer_id")
                .withColumn("sms_usage", col("count").cast(DataTypes.LongType));

        Dataset<Row> voiceUsage = spark.read()
                .format("mongodb")
                .options(MongoConfig.createReadOptions("analytics_voice_calls_total_duration"))
                .load()
                .withColumnRenamed("caller_id", "customer_id")
                .withColumn("voice_usage", col("total_duration").cast(DataTypes.LongType));

        // Fill null values with 0 for usage data
        dataUsage = dataUsage.na().fill(0.0, new String[] { "data_usage" });
        smsUsage = smsUsage.na().fill(0L, new String[] { "sms_usage" });
        voiceUsage = voiceUsage.na().fill(0L, new String[] { "voice_usage" });

        // Join all data together
        Dataset<Row> usage = customers
                .join(dataUsage, "customer_id", "left")
                .join(smsUsage, "customer_id", "left")
                .join(voiceUsage, "customer_id", "left")
                .join(ratePlans, customers.col("ratePlanIdLong").equalTo(ratePlans.col("ratePlanIdLong")), "left")
                .withColumnRenamed("productRates", "productRatesArray");

        // Register UDF for detailed rating calculation
        UserDefinedFunction ratingUDF = udf(
                (Long voice, Long sms, Double data, scala.collection.Seq<Row> productRates) -> {
                    double total = 0.0;
                    double voiceCharge = 0.0;
                    double smsCharge = 0.0;
                    double dataCharge = 0.0;

                    if (productRates == null) {
                        return RowFactory.create(0.0, 0.0, 0.0, 0.0);
                    }

                    for (Row productRate : scala.collection.JavaConversions.seqAsJavaList(productRates)) {
                        if (productRate == null)
                            continue;

                        int serviceType = productRate.getInt(0);
                        Object unitPriceObj = productRate.get(1);
                        double unitPrice = unitPriceObj instanceof Integer ? ((Integer) unitPriceObj).doubleValue()
                                : unitPriceObj instanceof Number ? ((Number) unitPriceObj).doubleValue() : 0.0;

                        Object freeUnitsObj = productRate.get(2);
                        long freeUnits = freeUnitsObj instanceof Integer ? ((Integer) freeUnitsObj).longValue()
                                : freeUnitsObj instanceof Number ? ((Number) freeUnitsObj).longValue() : 0L;

                        double usageAmount = 0;

                        // Get usage for the current service type
                        switch (serviceType) {
                            case 1: // Voice
                                usageAmount = voice != null ? voice : 0;
                                break;
                            case 2: // SMS
                                usageAmount = sms != null ? sms : 0;
                                break;
                            case 3: // Data
                                usageAmount = data != null ? data : 0;
                                break;
                        }

                        // Calculate chargeable units
                        double chargeableUnits = Math.max(0, usageAmount - freeUnits);
                        double serviceTotal = 0.0;

                        // Check for tiered pricing
                        Object tieredPricingObj = productRate.get(3);
                        if (tieredPricingObj != null && tieredPricingObj instanceof scala.collection.Seq) {
                            scala.collection.Seq<Row> tiers = (scala.collection.Seq<Row>) tieredPricingObj;
                            double remainingUnits = chargeableUnits;

                            // Sort tiers by upToUnits ascending
                            java.util.List<Row> sortedTiers = new java.util.ArrayList<>(
                                    scala.collection.JavaConversions.seqAsJavaList(tiers));
                            sortedTiers.sort((a, b) -> Long.compare(a.getLong(0), b.getLong(0)));

                            for (int i = 0; i < sortedTiers.size() && remainingUnits > 0; i++) {
                                Row tier = sortedTiers.get(i);
                                if (tier == null)
                                    continue;

                                long upTo = tier.getLong(0);
                                Object priceObj = tier.get(1);
                                double price = priceObj instanceof Integer ? ((Integer) priceObj).doubleValue()
                                        : priceObj instanceof Number ? ((Number) priceObj).doubleValue() : 0.0;

                                // Calculate the range for this tier
                                long previousUpTo = (i == 0) ? 0 : sortedTiers.get(i - 1).getLong(0);
                                long tierRange = upTo - previousUpTo;

                                // Calculate charge for this tier
                                double unitsInTier = Math.min(remainingUnits, tierRange);
                                serviceTotal += unitsInTier * price;
                                remainingUnits -= unitsInTier;
                            }

                            // Charge remaining units at the last tier's price
                            if (remainingUnits > 0 && !sortedTiers.isEmpty()) {
                                Row lastTier = sortedTiers.get(sortedTiers.size() - 1);
                                if (lastTier != null) {
                                    Object lastPriceObj = lastTier.get(1);
                                    double lastPrice = lastPriceObj instanceof Integer
                                            ? ((Integer) lastPriceObj).doubleValue()
                                            : lastPriceObj instanceof Number ? ((Number) lastPriceObj).doubleValue()
                                                    : 0.0;
                                    serviceTotal += remainingUnits * lastPrice;
                                }
                            }
                        } else {
                            // Simple unit pricing
                            serviceTotal = chargeableUnits * unitPrice;
                        }

                        // Accumulate to the appropriate service charge
                        switch (serviceType) {
                            case 1:
                                voiceCharge = serviceTotal;
                                break;
                            case 2:
                                smsCharge = serviceTotal;
                                break;
                            case 3:
                                dataCharge = serviceTotal;
                                break;
                        }

                        total += serviceTotal;
                    }

                    return RowFactory.create(total, voiceCharge, smsCharge, dataCharge);
                },
                DataTypes.createStructType(new org.apache.spark.sql.types.StructField[] {
                        DataTypes.createStructField("total", DataTypes.DoubleType, false),
                        DataTypes.createStructField("voice_charge", DataTypes.DoubleType, false),
                        DataTypes.createStructField("sms_charge", DataTypes.DoubleType, false),
                        DataTypes.createStructField("data_charge", DataTypes.DoubleType, false)
                }));

        spark.udf().register("detailedRatingUDF", ratingUDF);

        // Compute detailed charges
        Dataset<Row> result = usage.withColumn("charges",
                callUDF("detailedRatingUDF", col("voice_usage"), col("sms_usage"), col("data_usage"),
                        col("productRatesArray")))
                .select(
                        col("customer_id"),
                        col("customerName"),
                        col("subscriptionType"),
                        col("charges.total").alias("total_charge"),
                        col("charges.voice_charge").alias("voice_charge"),
                        col("charges.sms_charge").alias("sms_charge"),
                        col("charges.data_charge").alias("data_charge"),
                        col("voice_usage"),
                        col("sms_usage"),
                        col("data_usage"));

        // Write results to MongoDB
        result.write()
                .format("mongodb")
                .mode("overwrite")
                .options(MongoConfig.createWriteOptions("customer_bills"))
                .save();
    }

    public void close() {
        if (spark != null) {
            spark.close();
        }
    }
}