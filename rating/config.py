from pyspark.sql import SparkSession


def create_spark_session():
    """Create and configure a Spark session with MongoDB connector."""
    return (
        SparkSession.builder.appName("RatingProcessor")
        .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017")
        .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
        )
        .getOrCreate()
    )


def create_read_options(collection):
    """Create MongoDB read options for a specific collection."""
    return {"database": "mydatabase", "collection": collection}


def create_write_options(collection):
    """Create MongoDB write options for a specific collection."""
    return {"database": "mydatabase", "collection": collection, "mode": "overwrite"}
