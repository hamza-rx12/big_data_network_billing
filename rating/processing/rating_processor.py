from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    LongType,
    ArrayType,
    IntegerType,
)
import logging
from config import create_spark_session, create_read_options, create_write_options

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculate_charges(voice, sms, data, product_rates):
    """Calculate charges based on usage and product rates."""
    total = 0.0
    voice_charge = 0.0
    sms_charge = 0.0
    data_charge = 0.0

    # Ensure we have valid numbers
    voice_usage = voice if voice is not None else 0
    sms_usage = sms if sms is not None else 0
    data_usage = data if data is not None else 0.0

    logger.info(
        f"Starting calculation with values: Voice={voice_usage}, SMS={sms_usage}, Data={data_usage}"
    )

    if not product_rates:
        logger.info("No product rates found")
        return (0.0, 0.0, 0.0, 0.0)

    logger.info(f"Number of product rates: {len(product_rates)}")

    for rate in product_rates:
        if rate is None:
            logger.info("Skipping null product rate")
            continue

        # Extract values from the rate object
        service_type = rate.serviceType if hasattr(rate, "serviceType") else 0
        unit_price = (
            float(rate.unitPrice)
            if hasattr(rate, "unitPrice") and rate.unitPrice is not None
            else 0.0
        )
        free_units = (
            float(rate.freeUnitsPerCycle)
            if hasattr(rate, "freeUnitsPerCycle") and rate.freeUnitsPerCycle is not None
            else 0.0
        )
        tiered_pricing = rate.tieredPricing if hasattr(rate, "tieredPricing") else []

        logger.info(f"Processing service type: {service_type}")
        logger.info(f"Unit price: {unit_price}")
        logger.info(f"Free units: {free_units}")

        # Get usage for the current service type
        if service_type == 1:  # Voice
            usage_amount = voice_usage
        elif service_type == 2:  # SMS
            usage_amount = sms_usage
        elif service_type == 3:  # Data
            usage_amount = data_usage
        else:
            logger.info(f"Unknown service type: {service_type}")
            continue

        logger.info(f"Usage amount for service {service_type}: {usage_amount}")

        # Calculate chargeable units
        chargeable_units = max(0, usage_amount - free_units)
        logger.info(f"Chargeable units: {chargeable_units}")
        service_total = 0.0

        # Check for tiered pricing
        if tiered_pricing and isinstance(tiered_pricing, list):
            logger.info("Using tiered pricing")
            remaining_units = chargeable_units

            # Sort tiers by upToUnits ascending
            sorted_tiers = sorted(
                tiered_pricing,
                key=lambda x: x.upToUnits if hasattr(x, "upToUnits") else 0,
            )
            logger.info(f"Sorted tiers: {sorted_tiers}")

            for i, tier in enumerate(sorted_tiers):
                up_to = tier.upToUnits if hasattr(tier, "upToUnits") else 0
                price = (
                    float(tier.pricePerUnit)
                    if hasattr(tier, "pricePerUnit") and tier.pricePerUnit is not None
                    else 0.0
                )

                logger.info(f"Tier {i}: upTo={up_to}, price={price}")

                # Calculate the range for this tier
                previous_up_to = (
                    0
                    if i == 0
                    else (
                        sorted_tiers[i - 1].upToUnits
                        if hasattr(sorted_tiers[i - 1], "upToUnits")
                        else 0
                    )
                )
                tier_range = up_to - previous_up_to

                # Calculate charge for this tier
                units_in_tier = min(remaining_units, tier_range)
                service_total += units_in_tier * price
                remaining_units -= units_in_tier

                logger.info(
                    f"Units in tier: {units_in_tier}, charge: {units_in_tier * price}"
                )

            # Charge remaining units at the last tier's price
            if remaining_units > 0 and sorted_tiers:
                last_tier = sorted_tiers[-1]
                last_price = (
                    float(last_tier.pricePerUnit)
                    if hasattr(last_tier, "pricePerUnit")
                    and last_tier.pricePerUnit is not None
                    else 0.0
                )
                service_total += remaining_units * last_price
                logger.info(
                    f"Remaining units: {remaining_units}, charge: {remaining_units * last_price}"
                )
        else:
            # Simple unit pricing
            logger.info("Using simple unit pricing")
            service_total = chargeable_units * unit_price
            logger.info(f"Service total: {service_total}, unit price: {unit_price}")

        # Accumulate to the appropriate service charge
        if service_type == 1:
            voice_charge = service_total
        elif service_type == 2:
            sms_charge = service_total
        elif service_type == 3:
            data_charge = service_total

        total += service_total
        logger.info(f"Service type {service_type} total: {service_total}")

    logger.info(
        f"Final totals - Total: {total}, Voice: {voice_charge}, SMS: {sms_charge}, Data: {data_charge}"
    )
    return (total, voice_charge, sms_charge, data_charge)


class RatingProcessor:
    def __init__(self):
        self.spark = create_spark_session()

    def process_ratings(self):
        """Process ratings for all customers."""
        try:
            # Read all required datasets
            customers = (
                self.spark.read.format("mongodb")
                .options(**create_read_options("customers"))
                .load()
                .withColumn("ratePlanIdLong", F.col("ratePlanId").cast("long"))
                .withColumnRenamed("_id", "customer_id")
            )

            rate_plans = (
                self.spark.read.format("mongodb")
                .options(**create_read_options("ratePlan"))
                .load()
                .withColumn("ratePlanIdLong", F.col("ratePlanId"))
            )

            data_usage = (
                self.spark.read.format("mongodb")
                .options(**create_read_options("analytics_data_usage_total"))
                .load()
                .withColumnRenamed("user_id", "customer_id")
                .withColumn("data_usage", F.col("total_data_mb").cast("double"))
            )

            sms_usage = (
                self.spark.read.format("mongodb")
                .options(**create_read_options("analytics_sms_total_messages"))
                .load()
                .withColumnRenamed("sender_id", "customer_id")
                .withColumn("sms_usage", F.col("count").cast("long"))
            )

            voice_usage = (
                self.spark.read.format("mongodb")
                .options(**create_read_options("analytics_voice_calls_total_duration"))
                .load()
                .withColumnRenamed("caller_id", "customer_id")
                .withColumn("voice_usage", F.col("total_duration").cast("long"))
            )

            # Fill null values with 0 for usage data
            data_usage = data_usage.na.fill(0.0, ["data_usage"])
            sms_usage = sms_usage.na.fill(0, ["sms_usage"])
            voice_usage = voice_usage.na.fill(0, ["voice_usage"])

            logger.info("Starting rating calculation")

            # Join all data together
            usage = (
                customers.join(data_usage, "customer_id", "left")
                .join(sms_usage, "customer_id", "left")
                .join(voice_usage, "customer_id", "left")
                .join(
                    rate_plans,
                    customers.ratePlanIdLong == rate_plans.ratePlanIdLong,
                    "left",
                )
                .withColumnRenamed("productRates", "productRatesArray")
                .na.fill(0.0, ["data_usage"])
                .na.fill(0, ["sms_usage", "voice_usage"])
            )

            # Define the UDF function inline
            def calculate_charges(voice, sms, data, product_rates):
                total = 0.0
                voice_charge = 0.0
                sms_charge = 0.0
                data_charge = 0.0

                # Ensure we have valid numbers
                voice_usage = voice if voice is not None else 0
                sms_usage = sms if sms is not None else 0
                data_usage = data if data is not None else 0.0

                if not product_rates:
                    return (0.0, 0.0, 0.0, 0.0)

                for rate in product_rates:
                    if rate is None:
                        continue

                    # Extract values from the rate object
                    service_type = (
                        rate.serviceType if hasattr(rate, "serviceType") else 0
                    )
                    unit_price = (
                        float(rate.unitPrice)
                        if hasattr(rate, "unitPrice") and rate.unitPrice is not None
                        else 0.0
                    )
                    free_units = (
                        float(rate.freeUnitsPerCycle)
                        if hasattr(rate, "freeUnitsPerCycle")
                        and rate.freeUnitsPerCycle is not None
                        else 0.0
                    )
                    tiered_pricing = (
                        rate.tieredPricing if hasattr(rate, "tieredPricing") else []
                    )

                    # Get usage for the current service type
                    if service_type == 1:  # Voice
                        usage_amount = voice_usage
                    elif service_type == 2:  # SMS
                        usage_amount = sms_usage
                    elif service_type == 3:  # Data
                        usage_amount = data_usage
                    else:
                        continue

                    # Calculate chargeable units
                    chargeable_units = max(0, usage_amount - free_units)
                    service_total = 0.0

                    # Check for tiered pricing
                    if tiered_pricing and isinstance(tiered_pricing, list):
                        remaining_units = chargeable_units
                        sorted_tiers = sorted(
                            tiered_pricing,
                            key=lambda x: x.upToUnits if hasattr(x, "upToUnits") else 0,
                        )

                        for i, tier in enumerate(sorted_tiers):
                            up_to = tier.upToUnits if hasattr(tier, "upToUnits") else 0
                            price = (
                                float(tier.pricePerUnit)
                                if hasattr(tier, "pricePerUnit")
                                and tier.pricePerUnit is not None
                                else 0.0
                            )

                            # Calculate the range for this tier
                            previous_up_to = (
                                0
                                if i == 0
                                else (
                                    sorted_tiers[i - 1].upToUnits
                                    if hasattr(sorted_tiers[i - 1], "upToUnits")
                                    else 0
                                )
                            )
                            tier_range = up_to - previous_up_to

                            # Calculate charge for this tier
                            units_in_tier = min(remaining_units, tier_range)
                            service_total += units_in_tier * price
                            remaining_units -= units_in_tier

                        # Charge remaining units at the last tier's price
                        if remaining_units > 0 and sorted_tiers:
                            last_tier = sorted_tiers[-1]
                            last_price = (
                                float(last_tier.pricePerUnit)
                                if hasattr(last_tier, "pricePerUnit")
                                and last_tier.pricePerUnit is not None
                                else 0.0
                            )
                            service_total += remaining_units * last_price
                    else:
                        # Simple unit pricing
                        service_total = chargeable_units * unit_price

                    # Accumulate to the appropriate service charge
                    if service_type == 1:
                        voice_charge = service_total
                    elif service_type == 2:
                        sms_charge = service_total
                    elif service_type == 3:
                        data_charge = service_total

                    total += service_total

                return (total, voice_charge, sms_charge, data_charge)

            # Register the UDF
            calculate_charges_udf = F.udf(
                calculate_charges,
                StructType(
                    [
                        StructField("total", DoubleType(), False),
                        StructField("voice_charge", DoubleType(), False),
                        StructField("sms_charge", DoubleType(), False),
                        StructField("data_charge", DoubleType(), False),
                    ]
                ),
            )

            # Debug: Print sample of data before UDF call
            logger.info("Sample of data before UDF call:")
            usage.select(
                "customer_id",
                "voice_usage",
                "sms_usage",
                "data_usage",
                "productRatesArray",
            ).show(5, False)

            # Additional debug logging for data types
            logger.info("Data types of columns:")
            usage.printSchema()

            # Debug: Print sample of rate plans
            logger.info("Sample of rate plans:")
            rate_plans.select("ratePlanId", "productRates").show(5, False)

            # Debug: Print sample of raw data
            logger.info("Sample of raw data from MongoDB:")
            customers.show(5, False)
            data_usage.show(5, False)
            sms_usage.show(5, False)
            voice_usage.show(5, False)

            # Compute detailed charges
            result = usage.withColumn(
                "charges",
                calculate_charges_udf(
                    F.col("voice_usage"),
                    F.col("sms_usage"),
                    F.col("data_usage"),
                    F.col("productRatesArray"),
                ),
            ).select(
                F.col("customer_id"),
                F.col("customerName"),
                F.col("subscriptionType"),
                F.col("charges.total").alias("total_charge"),
                F.col("charges.voice_charge").alias("voice_charge"),
                F.col("charges.sms_charge").alias("sms_charge"),
                F.col("charges.data_charge").alias("data_charge"),
                F.col("voice_usage"),
                F.col("sms_usage"),
                F.col("data_usage"),
            )

            # Debug: Print sample of results after UDF call
            logger.info("Sample of results after UDF call:")
            result.show(5, False)

            # Write results to MongoDB
            result.write.format("mongodb").mode("overwrite").options(
                **create_write_options("customer_bills")
            ).save()

        except Exception as e:
            logger.error(f"Error processing ratings: {str(e)}")
            raise

    def close(self):
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()
