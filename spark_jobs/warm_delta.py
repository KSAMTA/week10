"""Runs once at image build time: downloads the Delta Lake jars into the ivy cache."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

b = (SparkSession.builder.master("local[1]").appName("warm")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(b).getOrCreate()
spark.range(1).write.format("delta").mode("overwrite").save("/tmp/warm_delta")
spark.stop()
