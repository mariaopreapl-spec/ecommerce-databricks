# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - generic Auto Loader ingestion
# MAGIC One parameterized notebook for all source tables. The `src` parameter (set by the job's For each loop) picks the table.
# MAGIC Auto Loader (`cloudFiles`) reads only the CSV files that have not been processed yet, and the checkpoint remembers what was loaded.
# MAGIC `availableNow` processes everything new and then stops (incremental batch). Schema changes go to `_rescued_data` instead of failing the load.

# COMMAND ----------

dbutils.widgets.text("src", "")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"/Volumes/ecommerce/bronze/bronzevolume/{src_value}/checkpoint")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load(f"/Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing/rawdata/{src_value}/")

# COMMAND ----------

df.writeStream.format("delta")\
        .outputMode("append")\
        .trigger(availableNow=True)\
        .option("checkpointLocation", f"/Volumes/ecommerce/bronze/bronzevolume/{src_value}/checkpoint")\
        .option("path", f"/Volumes/ecommerce/bronze/bronzevolume/{src_value}/data")\
        .start()
