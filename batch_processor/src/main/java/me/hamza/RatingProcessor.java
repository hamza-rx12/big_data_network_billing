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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatingProcessor {
    private final SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(RatingProcessor.class);

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

        logger.info("//////////////////////////////////////////////////////////////");
        logger.info("Starting rating calculation");
        logger.info("//////////////////////////////////////////////////////////////");

        // Join all data together with proper null handling
        Dataset<Row> usage = customers
                .join(dataUsage, "customer_id", "left")
                .join(smsUsage, "customer_id", "left")
                .join(voiceUsage, "customer_id", "left")
                .join(ratePlans, customers.col("ratePlanIdLong").equalTo(ratePlans.col("ratePlanIdLong")), "left")
                .withColumnRenamed("productRates", "productRatesArray")
                .na().fill(0.0, new String[] { "data_usage" })
                .na().fill(0L, new String[] { "sms_usage", "voice_usage" });

        // Debug: Print sample of data before UDF call
        logger.info("DEBUG: Sample of data before UDF call:");
        usage.select("customer_id", "voice_usage", "sms_usage", "data_usage", "productRatesArray")
                .show(5, false);

        // Debug: Print sample of rate plans
        logger.info("DEBUG: Sample of rate plans:");
        ratePlans.select("ratePlanId", "productRates").show(5, false);

        // Register UDF for detailed rating calculation
        UserDefinedFunction ratingUDF = udf(
                (Long voice, Long sms, Double data, scala.collection.Seq<Row> productRates) -> {
                    double total = 0.0;
                    double voiceCharge = 0.0;
                    double smsCharge = 0.0;
                    double dataCharge = 0.0;

                    // Ensure we have valid numbers
                    long voiceUsageValue = (voice != null) ? voice : 0L;
                    long smsUsageValue = (sms != null) ? sms : 0L;
                    double dataUsageValue = (data != null) ? data : 0.0;

                    logger.info("Starting calculation with values: Voice=" + voiceUsageValue +
                            ", SMS=" + smsUsageValue + ", Data=" + dataUsageValue);

                    if (productRates == null) {
                        logger.info("No product rates found");
                        return RowFactory.create(0.0, 0.0, 0.0, 0.0);
                    }

                    logger.info("Number of product rates: " + productRates.size());

                    for (Row rate : scala.collection.JavaConversions.seqAsJavaList(productRates)) {
                        if (rate == null) {
                            logger.info("Skipping null product rate");
                            continue;
                        }

                        // Extract values from the rate object
                        int freeUnits = rate.getInt(0); // freeUnitsPerCycle
                        int serviceType = rate.getInt(1); // serviceType
                        Object tieredPricingObj = rate.get(2); // tieredPricing
                        Object unitPriceObj = rate.get(3); // unitPrice
                        double unitPrice = 0.0;
                        if (unitPriceObj != null) {
                            if (unitPriceObj instanceof Integer) {
                                unitPrice = ((Integer) unitPriceObj).doubleValue();
                            } else if (unitPriceObj instanceof Double) {
                                unitPrice = (Double) unitPriceObj;
                            } else if (unitPriceObj instanceof Number) {
                                unitPrice = ((Number) unitPriceObj).doubleValue();
                            }
                        }

                        logger.info("Processing service type: " + serviceType);
                        logger.info("Unit price: " + unitPrice);
                        logger.info("Free units: " + freeUnits);

                        // Get usage for the current service type
                        double usageAmount = 0;
                        switch (serviceType) {
                            case 1: // Voice
                                usageAmount = voiceUsageValue;
                                break;
                            case 2: // SMS
                                usageAmount = smsUsageValue;
                                break;
                            case 3: // Data
                                usageAmount = dataUsageValue;
                                break;
                            default:
                                logger.info("Unknown service type: " + serviceType);
                                continue;
                        }

                        logger.info("Usage amount for service " + serviceType + ": " + usageAmount);

                        // Calculate chargeable units
                        double chargeableUnits = Math.max(0, usageAmount - freeUnits);
                        logger.info("Chargeable units: " + chargeableUnits);
                        double serviceTotal = 0.0;

                        // Check for tiered pricing
                        if (tieredPricingObj != null && tieredPricingObj instanceof scala.collection.Seq) {
                            logger.info("Using tiered pricing");
                            scala.collection.Seq<Row> tiers = (scala.collection.Seq<Row>) tieredPricingObj;
                            double remainingUnits = chargeableUnits;

                            // Sort tiers by upToUnits ascending
                            java.util.List<Row> sortedTiers = new java.util.ArrayList<>(
                                    scala.collection.JavaConversions.seqAsJavaList(tiers));
                            sortedTiers.sort((a, b) -> {
                                Object aObj = a.get(0);
                                Object bObj = b.get(0);
                                double aVal = (aObj instanceof Number) ? ((Number) aObj).doubleValue() : 0.0;
                                double bVal = (bObj instanceof Number) ? ((Number) bObj).doubleValue() : 0.0;
                                return Double.compare(aVal, bVal);
                            });

                            logger.info("Sorted tiers: " + sortedTiers);

                            for (int i = 0; i < sortedTiers.size() && remainingUnits > 0; i++) {
                                Row tier = sortedTiers.get(i);
                                if (tier == null)
                                    continue;

                                Object upToObj = tier.get(0);
                                Object priceObj = tier.get(1);
                                double upTo = (upToObj instanceof Number) ? ((Number) upToObj).doubleValue() : 0.0;
                                double price = (priceObj instanceof Number) ? ((Number) priceObj).doubleValue() : 0.0;

                                logger.info("Tier " + i + ": upTo=" + upTo + ", price=" + price);

                                // Calculate the range for this tier
                                double previousUpTo = 0.0;
                                if (i > 0) {
                                    Object prevUpToObj = sortedTiers.get(i - 1).get(0);
                                    previousUpTo = (prevUpToObj instanceof Number)
                                            ? ((Number) prevUpToObj).doubleValue()
                                            : 0.0;
                                }
                                double tierRange = upTo - previousUpTo;

                                // Calculate charge for this tier
                                double unitsInTier = Math.min(remainingUnits, tierRange);
                                serviceTotal += unitsInTier * price;
                                remainingUnits -= unitsInTier;

                                logger.info("Units in tier: " + unitsInTier + ", charge: " + (unitsInTier * price));
                            }

                            // Charge remaining units at the last tier's price
                            if (remainingUnits > 0 && !sortedTiers.isEmpty()) {
                                Row lastTier = sortedTiers.get(sortedTiers.size() - 1);
                                if (lastTier != null) {
                                    Object lastPriceObj = lastTier.get(1);
                                    double lastPrice = (lastPriceObj instanceof Number)
                                            ? ((Number) lastPriceObj).doubleValue()
                                            : 0.0;
                                    serviceTotal += remainingUnits * lastPrice;
                                    logger.info("Remaining units: " + remainingUnits + ", charge: "
                                            + (remainingUnits * lastPrice));
                                }
                            }
                        } else {
                            // Simple unit pricing
                            logger.info("Using simple unit pricing");
                            serviceTotal = chargeableUnits * unitPrice;
                            logger.info("Service total: " + serviceTotal + ", unit price: " + unitPrice);
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
                        logger.info("Service type " + serviceType + " total: " + serviceTotal);
                    }

                    logger.info("Final totals - Total: " + total + ", Voice: " + voiceCharge +
                            ", SMS: " + smsCharge + ", Data: " + dataCharge);
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

        // Debug: Print sample of results after UDF call
        logger.info("DEBUG: Sample of results after UDF call:");
        result.show(5, false);

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