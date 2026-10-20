# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - real-time order stream (JSON)
# MAGIC Ingests the live order events written by `data_generator/stream_orders.py` into the landing volume.
# MAGIC
# MAGIC Unlike the batch sources (flat CSV), every event is a **nested JSON document**: an order header with an embedded `items` array.
# MAGIC Bronze keeps it exactly as received. Flattening and exploding happen in silver.
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`, format `json`) picks up only the files that are new since the last run.
# MAGIC - **Schema inference** with `inferColumnTypes` turns `items` into an `array<struct<...>>` and the amounts into numbers.
# MAGIC - **Schema hints** pin the business keys and the timestamp to `string`, so a malformed event cannot change the column type.
# MAGIC - **Lineage columns:** each row records the file it came from and when it was loaded.
# MAGIC - **`availableNow`** processes everything waiting in the folder and then stops, so it can run in the same job as the batch sources. Switch the trigger to `processingTime="10 seconds"` to keep it running continuously.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

LANDING = "/Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing/stream_landing/"
TARGET = "/Volumes/ecommerce/bronze/bronzevolume/orders_stream"

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{TARGET}/checkpoint")
    .option("cloudFiles.inferColumnTypes", "true")
    .option(
        "cloudFiles.schemaHints",
        "order_id STRING, customer_id STRING, order_timestamp STRING",
    )
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(LANDING)
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_bronze_loaded_at", current_timestamp())
)

# COMMAND ----------

(
    df.writeStream.format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", f"{TARGET}/checkpoint")
    .option("path", f"{TARGET}/data")
    .start()
)
